import os
import subprocess
import json
from datetime import datetime

def check_docker_containers():
    """
    Vérifie que les conteneurs Hadoop sont en cours d'exécution
    """
    print("Vérification des conteneurs Docker...")
    
    result = subprocess.run(['docker', 'ps'], capture_output=True, text=True)
    if result.returncode != 0:
        print("Erreur: Docker n'est pas accessible")
        return False
    
    running_containers = result.stdout
    required_containers = ['namenode', 'datanode']
    
    for container in required_containers:
        if container not in running_containers:
            print(f"Erreur: Le conteneur {container} n'est pas en cours d'exécution")
            print("Démarrez votre cluster avec: docker-compose up -d")
            return False
    
    print("Conteneurs Hadoop en cours d'exécution")
    return True

def push_to_hdfs(local_dir="tweets_organized", hdfs_base="/user/data/tweets"):
    """
    Pousse la structure locale vers HDFS
    """
    print("=== Push vers HDFS ===")
    
    if not os.path.exists(local_dir):
        print(f"Erreur: Le dossier local {local_dir} n'existe pas")
        return False
    
    if not check_docker_containers():
        return False
    
    # Supprimer le répertoire HDFS s'il existe déjà
    print(f"Nettoyage du répertoire HDFS: {hdfs_base}")
    subprocess.run([
        'docker', 'exec', 'namenode', 
        'hdfs', 'dfs', '-rm', '-r', '-f', hdfs_base
    ], capture_output=True)
    
    # Créer la structure de base dans HDFS
    print(f"Création de la structure de base dans HDFS: {hdfs_base}")
    result = subprocess.run([
        'docker', 'exec', 'namenode', 
        'hdfs', 'dfs', '-mkdir', '-p', hdfs_base
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Erreur lors de la création du répertoire HDFS: {result.stderr}")
        return False
    
    successful_transfers = 0
    failed_transfers = 0
    total_tweets = 0
    
    # Parcourir la structure locale
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            if file.endswith('.json') and file != 'statistics.json':
                local_file_path = os.path.join(root, file)
                
                # Construire le chemin HDFS correspondant
                relative_path = os.path.relpath(local_file_path, local_dir)
                hdfs_dir = os.path.join(hdfs_base, os.path.dirname(relative_path)).replace('\\', '/')
                hdfs_file_path = os.path.join(hdfs_dir, file).replace('\\', '/')
                
                # Créer le répertoire dans HDFS
                subprocess.run([
                    'docker', 'exec', 'namenode',
                    'hdfs', 'dfs', '-mkdir', '-p', hdfs_dir
                ], capture_output=True)
                
                # Copier le fichier vers le conteneur
                container_temp_path = f"/tmp/tweet_temp_{successful_transfers + failed_transfers}.json"
                copy_result = subprocess.run([
                    'docker', 'cp', local_file_path, f'namenode:{container_temp_path}'
                ], capture_output=True, text=True)
                
                if copy_result.returncode != 0:
                    print(f"Erreur copie vers conteneur: {relative_path}")
                    failed_transfers += 1
                    continue
                
                # Pousser vers HDFS
                hdfs_result = subprocess.run([
                    'docker', 'exec', 'namenode',
                    'hdfs', 'dfs', '-put', container_temp_path, hdfs_file_path
                ], capture_output=True, text=True)
                
                if hdfs_result.returncode == 0:
                    # Compter les tweets dans le fichier
                    try:
                        with open(local_file_path, 'r', encoding='utf-8') as f:
                            tweets = json.load(f)
                        tweet_count = len(tweets)
                        total_tweets += tweet_count
                        
                        print(f"{relative_path}: {tweet_count} tweets -> {hdfs_file_path}")
                        successful_transfers += 1
                    except Exception as e:
                        print(f"{relative_path}: transféré mais erreur de comptage - {e}")
                        successful_transfers += 1
                else:
                    print(f"Erreur HDFS pour {relative_path}: {hdfs_result.stderr}")
                    failed_transfers += 1
                
                # Nettoyer le fichier temporaire
                subprocess.run([
                    'docker', 'exec', 'namenode', 'rm', '-f', container_temp_path
                ], capture_output=True)
    
    print(f"\nRésumé du transfert:")
    print(f"Fichiers transférés avec succès: {successful_transfers}")
    print(f"Fichiers en échec: {failed_transfers}")
    print(f"Total tweets transférés: {total_tweets}")
    
    if successful_transfers > 0:
        print(f"\nPush terminé avec succès!")
        return True
    else:
        print(f"\nAucun fichier transféré avec succès")
        return False

def verify_hdfs_structure(hdfs_base="/user/data/tweets"):
    """
    Vérifie la structure créée dans HDFS
    """
    print("\n=== Vérification de la structure HDFS ===")
    
    # Lister la structure complète
    result = subprocess.run([
        'docker', 'exec', 'namenode',
        'hdfs', 'dfs', '-ls', '-R', hdfs_base
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Erreur lors de la vérification: {result.stderr}")
        return
    
    print("Structure HDFS créée:")
    print(result.stdout)

def verify_hdfs_content_sample(hdfs_base="/user/data/tweets"):
    """
    Vérifie un échantillon du contenu des fichiers dans HDFS
    """
    print("\n=== Vérification d'échantillon de contenu ===")
    
    # Trouver un fichier exemple
    result = subprocess.run([
        'docker', 'exec', 'namenode',
        'hdfs', 'dfs', '-find', hdfs_base, '-name', '*.json'
    ], capture_output=True, text=True)
    
    if result.returncode != 0 or not result.stdout.strip():
        print("Aucun fichier JSON trouvé dans HDFS")
        return
    
    # Prendre le premier fichier trouvé
    sample_file = result.stdout.strip().split('\n')[0]
    print(f"Échantillon du fichier: {sample_file}")
    
    # Lire les premières lignes
    content_result = subprocess.run([
        'docker', 'exec', 'namenode',
        'hdfs', 'dfs', '-cat', sample_file
    ], capture_output=True, text=True)
    
    if content_result.returncode == 0:
        try:
            content = content_result.stdout
            # Afficher les premiers caractères pour vérifier
            print("Aperçu du contenu (200 premiers caractères):")
            print(content[:200] + "..." if len(content) > 200 else content)
            
            # Essayer de parser le JSON pour validation
            tweets = json.loads(content)
            print(f"Validation JSON réussie: {len(tweets)} tweets dans ce fichier")
            
            if tweets:
                print("Exemple de tweet:")
                sample_tweet = tweets[0]
                for key, value in list(sample_tweet.items())[:5]:  # Afficher 5 premiers champs
                    print(f"  {key}: {value}")
                    
        except json.JSONDecodeError as e:
            print(f"Erreur parsing JSON: {e}")
        except Exception as e:
            print(f"Erreur lecture contenu: {e}")
    else:
        print(f"Erreur lecture du fichier: {content_result.stderr}")

def generate_hdfs_report(hdfs_base="/user/data/tweets"):
    """
    Génère un rapport sur les données dans HDFS
    """
    print("\n=== Rapport HDFS ===")
    
    # Compter les fichiers et calculer la taille
    result = subprocess.run([
        'docker', 'exec', 'namenode',
        'hdfs', 'dfs', '-du', '-s', '-h', hdfs_base
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        size_info = result.stdout.strip()
        print(f"Taille totale des données: {size_info}")
    
    # Compter les partitions
    partitions_result = subprocess.run([
        'docker', 'exec', 'namenode',
        'hdfs', 'dfs', '-ls', hdfs_base + '/year=2024'
    ], capture_output=True, text=True)
    
    if partitions_result.returncode == 0:
        partitions = [line for line in partitions_result.stdout.split('\n') if 'month=' in line]
        print(f"Nombre de partitions mensuelles: {len(partitions)}")

if __name__ == "__main__":
    # Paramètres
    local_directory = "tweets_organized"
    hdfs_directory = "/user/data/tweets"
    
    print("=== Transfert des tweets vers HDFS ===")
    
    # Vérifier que la structure locale existe
    if not os.path.exists(local_directory):
        print(f"Le dossier local '{local_directory}' n'existe pas.")
        print("Exécutez d'abord: python organize_tweets.py")
        exit(1)
    
    # Pousser vers HDFS
    success = push_to_hdfs(local_directory, hdfs_directory)
    
    if success:
        # Vérifications
        verify_hdfs_structure(hdfs_directory)
        verify_hdfs_content_sample(hdfs_directory)
        generate_hdfs_report(hdfs_directory)
        
        print(f"\nDonnées organisées avec succès dans HDFS!")
        print(f"Localisation: {hdfs_directory}")
        print(f"Structure: {hdfs_directory}/year=2024/month=XX/tweets.json")
    else:
        print("Échec du transfert vers HDFS")
        exit(1)