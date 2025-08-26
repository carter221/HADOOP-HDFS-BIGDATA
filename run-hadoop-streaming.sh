#!/usr/bin/env bash
# Script d'exécution Hadoop Streaming depuis le host via le container 'namenode'
set -euo pipefail

CONTAINER=namenode
#!/usr/bin/env bash
# Script d'exécution Hadoop Streaming depuis le host via le container 'namenode'
set -euo pipefail

CONTAINER=namenode
# Par défaut utilisez le chemin réel présent dans HDFS
HDFS_INPUT=/user/data/tweets
ALT_HDFS_INPUT=/user/data/tweet
HDFS_OUTPUT_BASE=/user/data/output/mapreduce_results

LOCAL_DIR=$(pwd)
MAPPER=mapper.py
REDUCER=reducer.py

echo "Copie des scripts dans le container $CONTAINER..."
docker cp "$LOCAL_DIR/$MAPPER" $CONTAINER:/tmp/mapper.py
docker cp "$LOCAL_DIR/$REDUCER" $CONTAINER:/tmp/reducer.py
docker exec $CONTAINER chmod +x /tmp/mapper.py /tmp/reducer.py

# Chemin connu (fourni par l'utilisateur)
KNOWN_JAR='/opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar'
STREAMING_JAR=""
echo "Vérification du hadoop-streaming jar connu: $KNOWN_JAR"
if docker exec $CONTAINER bash -lc "[ -f $KNOWN_JAR ] && echo OK || true" | grep -q OK; then
  STREAMING_JAR=$KNOWN_JAR
  echo "Utilisation du hadoop-streaming jar connu: $STREAMING_JAR"
else
  echo "Jar connu non trouvé, recherche dans emplacements communs..."
  for pattern in \
    '/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar' \
    '/opt/hadoop-*/share/hadoop/tools/lib/hadoop-streaming-*.jar' \
    '/opt/hadoop-*/share/hadoop/tools/lib/*streaming*.jar' \
    '/opt/hadoop-*/share/hadoop/mapreduce/hadoop-streaming-*.jar' \
    '/opt/hadoop/share/hadoop/mapreduce/hadoop-streaming-*.jar' ; do
    cand=$(docker exec $CONTAINER bash -lc "ls $pattern 2>/dev/null || true" | head -n1)
    if [ -n "$cand" ]; then
      STREAMING_JAR=$cand
      break
    fi
  done
  if [ -z "$STREAMING_JAR" ]; then
    echo "Impossible de trouver hadoop-streaming jar dans le container. Chemins essayés:"
    echo "  /opt/hadoop*/share/hadoop/tools/lib/"
    echo "  /opt/hadoop*/share/hadoop/mapreduce/"
    echo "Vérifiez l'image Hadoop ou installez hadoop-streaming.jar dans le container."
    exit 1
  fi
  echo "hadoop-streaming jar trouvé: $STREAMING_JAR"
fi

echo "Recherche du binaire hdfs dans le container..."
HDFS_BIN=""
for pattern in \
  '/opt/hadoop/bin/hdfs' \
  '/opt/hadoop-*/bin/hdfs' \
  '/opt/hadoop-*/sbin/hdfs' \
  '/opt/hadoop-*/bin/hdfs' \
  '/usr/local/hadoop/bin/hdfs' \
  '/usr/bin/hdfs' ; do
  cand=$(docker exec $CONTAINER bash -lc "ls $pattern 2>/dev/null || true" | head -n1)
  if [ -n "$cand" ]; then
    HDFS_BIN=$cand
    break
  fi
done

if [ -z "$HDFS_BIN" ]; then
  echo "WARN: binaire 'hdfs' introuvable dans le container. Les opérations HDFS risquent d'échouer."
  echo "Vous pouvez exécuter 'python local-to-hdfs.py' depuis l'hôte pour pousser les données, ou installer Hadoop/HDFS dans le container."
else
  echo "hdfs trouvé: $HDFS_BIN"
fi

run_job(){
  local task=$1
  local outdir="$HDFS_OUTPUT_BASE/$task"
  echo "Suppression de $outdir si existe..."
  if [ -n "$HDFS_BIN" ]; then
    docker exec $CONTAINER $HDFS_BIN dfs -rm -r -f $outdir || true
  else
    docker exec $CONTAINER hdfs dfs -rm -r -f $outdir || true
  fi

  echo "Lancement job Hadoop Streaming pour $task..."
  # Construire la liste des fichiers d'entrée (tweets.json) et passer chacun en -input
  INPUT_ARGS=""
  if [ -n "$HDFS_BIN" ]; then
    cand_files=$(docker exec $CONTAINER bash -lc "$HDFS_BIN dfs -find $HDFS_INPUT -name 'tweets.json' 2>/dev/null || true")
    if [ -z "$cand_files" ]; then
      # fallback : parser le listing récursif
      cand_files=$(docker exec $CONTAINER bash -lc "$HDFS_BIN dfs -ls -R $HDFS_INPUT 2>/dev/null | awk '/tweets.json\\$/ {print \\\$8}' || true")
    fi
    if [ -n "$cand_files" ]; then
      # build -input args
      while read -r f; do
        [ -z "$f" ] && continue
        INPUT_ARGS="$INPUT_ARGS -input $f"
      done <<< "$cand_files"
    fi
  fi
  if [ -z "$INPUT_ARGS" ]; then
    INPUT_ARGS="-input $HDFS_INPUT"
  fi

  docker exec $CONTAINER hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="mapreduce_${task}" \
    -files /tmp/mapper.py,/tmp/reducer.py \
    -cmdenv ANALYSIS_TYPE="${task}" \
    -mapper "/tmp/mapper.py" \
    -reducer "/tmp/reducer.py" \
    $INPUT_ARGS \
    -output $outdir \
    -outputformat org.apache.hadoop.mapred.TextOutputFormat

  # Renommer le fichier de sortie pour avoir l'extension .txt
  echo "Renommage du fichier de sortie avec extension .txt..."
  if [ -n "$HDFS_BIN" ]; then
    docker exec $CONTAINER $HDFS_BIN dfs -mv "$outdir/part-00000" "$outdir/results_${task}.txt" || true
  else
    docker exec $CONTAINER hdfs dfs -mv "$outdir/part-00000" "$outdir/results_${task}.txt" || true
  fi

  echo "Job $task terminé. Résultats HDFS: $outdir/results_${task}.txt"
}

echo "Exécuter: run_job hashtags|sentiment|geography"
if [ $# -lt 1 ]; then
  echo "Usage: $0 <hashtags|sentiment|geography|all>"
  exit 1
fi

if [ "$1" = "all" ]; then
  run_job hashtags
  run_job sentiment
  run_job geography
else
  # before running, verify HDFS input exists or try alternate
  echo "Vérification du chemin d'entrée HDFS: $HDFS_INPUT"
  if [ -n "$HDFS_BIN" ]; then
    exists=$(docker exec $CONTAINER bash -lc "$HDFS_BIN dfs -test -e $HDFS_INPUT && echo OK || echo NO") || true
  else
    exists="NO"
  fi
  if [ "$exists" != "OK" ]; then
    echo "Chemin $HDFS_INPUT introuvable dans HDFS, test de l'alternative: $ALT_HDFS_INPUT"
    if [ -n "$HDFS_BIN" ]; then
      alt_exists=$(docker exec $CONTAINER bash -lc "$HDFS_BIN dfs -test -e $ALT_HDFS_INPUT && echo OK || echo NO") || true
    else
      alt_exists="NO"
    fi
    if [ "$alt_exists" = "OK" ]; then
      echo "Utilisation de $ALT_HDFS_INPUT comme entrée HDFS"
      HDFS_INPUT=$ALT_HDFS_INPUT
    else
      echo "Aucun chemin d'entrée trouvé. Poussez d'abord les données vers HDFS avec :"
      echo "  python local-to-hdfs.py"
      echo "ou créez manuellement $HDFS_INPUT"
      exit 2
    fi
  fi
  run_job $1
fi

echo "Done"
