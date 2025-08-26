#!/usr/bin/env python3
"""
Script optimisé pour pousser les 3 fichiers JSON d'analyse MapReduce vers HDFS
"""
import os
import subprocess
import json
from datetime import datetime
from pathlib import Path

class OptimizedHDFSUploader:
    """
    Classe optimisée pour gérer l'upload des 3 fichiers JSON d'analyse
    """
    
    def __init__(self, hdfs_base_path="/user/data/output"):
        self.hdfs_base_path = hdfs_base_path
        # Fichiers JSON spécifiques attendus
        self.target_files = {
            'tweets_with_locations.json': 'organized_data',
            'sentiment_analysis.json': 'sentiment', 
            'geographic_analysis.json': 'geographic'
        }
        
    def check_hdfs_connectivity(self):
        """
        Vérifie la connectivité HDFS
        """
        print("=== Vérification de la connectivité HDFS ===")
        
        try:
            # Essai avec Docker d'abord (plus probable dans votre environnement)
            result = subprocess.run([
                'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/'
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                print("HDFS accessible via Docker")
                self.use_docker = True
                return True
            else:
                print("Docker non accessible, essai HDFS direct...")
                return self.check_hdfs_direct()
                
        except Exception as e:
            print(f"Docker non disponible: {e}")
            return self.check_hdfs_direct()
    
    def check_hdfs_direct(self):
        """
        Vérifie HDFS en mode direct
        """
        try:
            result = subprocess.run([
                'hdfs', 'dfs', '-ls', '/'
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                print("HDFS accessible en direct")
                self.use_docker = False
                return True
            else:
                print(f"Erreur HDFS: {result.stderr}")
                return False
                
        except FileNotFoundError:
            print("Commande 'hdfs' non trouvée")
            return False
        except Exception as e:
            print(f"Exception: {e}")
            return False
    
    def run_hdfs_command(self, command):
        """
        Exécute une commande HDFS selon la méthode disponible
        """
        try:
            if self.use_docker:
                cmd = ['docker', 'exec', 'namenode'] + command
            else:
                cmd = command
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            return result.returncode == 0, result.stdout, result.stderr
            
        except Exception as e:
            return False, "", str(e)
    
    def check_path_exists(self, hdfs_path):
        """
        Vérifie si un chemin existe dans HDFS
        """
        success, stdout, stderr = self.run_hdfs_command(['hdfs', 'dfs', '-test', '-e', hdfs_path])
        return success
    
    def create_complete_hdfs_structure(self):
        """
        Crée la structure complète HDFS nécessaire avant l'upload
        """
        print("\n=== Création de la structure HDFS complète ===")
        
        # Liste de tous les répertoires à créer dans l'ordre hiérarchique
        directories = [
            "/user",
            "/user/data",
            f"{self.hdfs_base_path}",
            f"{self.hdfs_base_path}/mapreduce_results",
            f"{self.hdfs_base_path}/mapreduce_results/sentiment",
            f"{self.hdfs_base_path}/mapreduce_results/geographic", 
            f"{self.hdfs_base_path}/mapreduce_results/organized_data",
            f"{self.hdfs_base_path}/metadata"
        ]
        
        created_count = 0
        existing_count = 0
        failed_count = 0
        
        for directory in directories:
            print(f"Traitement: {directory}")
            
            # Vérifier si le répertoire existe déjà
            if self.check_path_exists(directory):
                print(f"  Existe déjà")
                existing_count += 1
                continue
            
            # Créer le répertoire
            success, stdout, stderr = self.run_hdfs_command(['hdfs', 'dfs', '-mkdir', '-p', directory])
            
            if success:
                print(f"  Créé avec succès")
                created_count += 1
            else:
                print(f"  Échec de création: {stderr}")
                failed_count += 1
        
        print(f"\nRésumé de la création de structure:")
        print(f"  Créés: {created_count}")
        print(f"  Existants: {existing_count}")
        print(f"  Échecs: {failed_count}")
        
        # Vérifier la structure finale
        print(f"\nVérification de la structure finale...")
        all_exist = True
        for directory in directories:
            exists = self.check_path_exists(directory)
            status = "OK" if exists else "ÉCHEC"
            print(f"  {status} {directory}")
            if not exists:
                all_exist = False
        
        if all_exist:
            print("Toute la structure HDFS est en place!")
            
            # Configurer les permissions
            self.fix_hdfs_permissions()
            
            return True
        else:
            print("Certains répertoires n'ont pas pu être créés")
            return False
    
    def fix_hdfs_permissions(self):
        """
        Configure les permissions HDFS appropriées
        """
        print("\n=== Configuration des permissions HDFS ===")
        
        base_paths = ["/user", "/user/data", self.hdfs_base_path]
        
        for path in base_paths:
            success, stdout, stderr = self.run_hdfs_command([
                'hdfs', 'dfs', '-chmod', '-R', '755', path
            ])
            
            if success:
                print(f"  Permissions 755 appliquées: {path}")
            else:
                print(f"  Avertissement permissions {path}: {stderr}")
    
    def find_target_files(self):
        """
        Trouve les 3 fichiers JSON cibles
        """
        print("\n=== Recherche des fichiers JSON cibles ===")
        
        found_files = {}
        missing_files = []
        
        # Recherche dans le répertoire courant et json-output/
        search_paths = ['.', 'json-output', 'TP-noté/json-output']
        
        for filename in self.target_files.keys():
            file_found = False
            
            for search_path in search_paths:
                file_path = os.path.join(search_path, filename)
                if os.path.exists(file_path):
                    size = os.path.getsize(file_path)
                    found_files[filename] = {
                        'path': file_path,
                        'size': size,
                        'category': self.target_files[filename]
                    }
                    print(f"  {filename} -> {file_path} ({self.format_size(size)})")
                    file_found = True
                    break
            
            if not file_found:
                missing_files.append(filename)
                print(f"  {filename} -> NON TROUVÉ")
        
        print(f"\nRésumé: {len(found_files)}/3 fichiers trouvés")
        
        if missing_files:
            print(f"Fichiers manquants: {', '.join(missing_files)}")
            return None
        
        return found_files
    
    def format_size(self, size_bytes):
        """
        Formate la taille en unités lisibles
        """
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024**2:
            return f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024**3:
            return f"{size_bytes/(1024**2):.1f} MB"
        else:
            return f"{size_bytes/(1024**3):.1f} GB"
    
    def create_hdfs_structure(self):
        """
        Ancienne méthode - remplacée par create_complete_hdfs_structure
        """
        return self.create_complete_hdfs_structure()
    
    def create_hdfs_directory(self, path):
        """
        Crée un répertoire HDFS - méthode simplifiée
        """
        success, stdout, stderr = self.run_hdfs_command(['hdfs', 'dfs', '-mkdir', '-p', path])
        return success
    
    def validate_json_content(self, file_path):
        """
        Validation simplifiée du contenu JSON
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            filename = os.path.basename(file_path)
            print(f"  JSON valide: {filename}")
            
            # Afficher quelques infos sur le contenu
            if filename == 'sentiment_analysis.json':
                total_days = data.get('total_days', 0)
                print(f"    Contient {total_days} jours d'analyse")
            elif filename == 'geographic_analysis.json':
                countries = data.get('total_countries', 0)
                cities = data.get('total_cities', 0)
                print(f"    Contient {countries} pays et {cities} villes")
            elif filename == 'tweets_with_locations.json':
                if isinstance(data, list):
                    print(f"    Contient {len(data)} tweets avec localisation")
            
            return True
            
        except json.JSONDecodeError as e:
            print(f"  JSON invalide: {file_path} - {e}")
            return False
        except Exception as e:
            print(f"  Erreur validation {file_path}: {e}")
            return True  # Continuer malgré l'erreur
    
    def upload_file_to_hdfs(self, local_file, hdfs_path):
        """
        Upload un fichier vers HDFS avec vérification préalable du répertoire parent
        """
        try:
            # Vérifier et créer le répertoire parent si nécessaire
            parent_dir = '/'.join(hdfs_path.split('/')[:-1])
            if not self.check_path_exists(parent_dir):
                print(f"    Création du répertoire parent: {parent_dir}")
                success, stdout, stderr = self.run_hdfs_command(['hdfs', 'dfs', '-mkdir', '-p', parent_dir])
                if not success:
                    return False, f"Impossible de créer le répertoire parent: {stderr}"
            
            if self.use_docker:
                # Méthode Docker optimisée
                container_path = f"/tmp/{os.path.basename(local_file)}"
                
                # 1. Copier vers le conteneur
                print(f"    Copie vers conteneur...")
                copy_cmd = ['docker', 'cp', local_file, f'namenode:{container_path}']
                result = subprocess.run(copy_cmd, capture_output=True, text=True, timeout=60)
                
                if result.returncode != 0:
                    return False, f"Erreur copie conteneur: {result.stderr}"
                
                # 2. Copier vers HDFS
                print(f"    Upload vers HDFS...")
                hdfs_cmd = ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', '-f', container_path, hdfs_path]
                result = subprocess.run(hdfs_cmd, capture_output=True, text=True, timeout=60)
                
                # 3. Nettoyer
                cleanup_cmd = ['docker', 'exec', 'namenode', 'rm', '-f', container_path]
                subprocess.run(cleanup_cmd, capture_output=True)
                
            else:
                # Méthode directe
                print(f"    Upload direct vers HDFS...")
                cmd = ['hdfs', 'dfs', '-put', '-f', local_file, hdfs_path]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                print(f"    Upload réussi")
                return True, "Succès"
            else:
                return False, result.stderr.strip()
                
        except subprocess.TimeoutExpired:
            return False, "Timeout (60s)"
        except Exception as e:
            return False, str(e)
    
    def test_hdfs_write_access(self):
        """
        Test l'accès en écriture HDFS avec un fichier temporaire
        """
        print("\n=== Test d'accès en écriture HDFS ===")
        
        # Créer un fichier de test
        test_content = {
            "test": True,
            "timestamp": datetime.now().isoformat(),
            "message": "Test d'accès HDFS"
        }
        
        test_file = "test_hdfs_access.json"
        try:
            with open(test_file, 'w') as f:
                json.dump(test_content, f, indent=2)
            
            # Tenter l'upload
            test_hdfs_path = f"{self.hdfs_base_path}/test_file.json"
            success, message = self.upload_file_to_hdfs(test_file, test_hdfs_path)
            
            if success:
                print("Test d'écriture HDFS réussi")
                
                # Nettoyer le fichier de test
                self.run_hdfs_command(['hdfs', 'dfs', '-rm', '-f', test_hdfs_path])
                
                # Nettoyer le fichier local
                if os.path.exists(test_file):
                    os.remove(test_file)
                
                return True
            else:
                print(f"Test d'écriture HDFS échoué: {message}")
                return False
                
        except Exception as e:
            print(f"Erreur test HDFS: {e}")
            return False
        finally:
            # Nettoyer le fichier local en cas d'erreur
            if os.path.exists(test_file):
                os.remove(test_file)
    
    def upload_target_files(self, found_files):
        """
        Upload les 3 fichiers JSON vers HDFS
        """
        print("\n=== Upload des fichiers MapReduce vers HDFS ===")
        
        upload_summary = {
            'total': len(found_files),
            'success': 0,
            'failed': 0,
            'details': []
        }
        
        for i, (filename, file_info) in enumerate(found_files.items(), 1):
            print(f"\nUpload {i}/3: {filename}")
            print(f"  Fichier local: {file_info['path']}")
            print(f"  Taille: {self.format_size(file_info['size'])}")
            
            # Validation du contenu
            if not self.validate_json_content(file_info['path']):
                print(f"  Validation échouée, fichier ignoré")
                upload_summary['failed'] += 1
                continue
            
            # Construction du chemin HDFS
            hdfs_path = f"{self.hdfs_base_path}/mapreduce_results/{file_info['category']}/{filename}"
            print(f"  Destination HDFS: {hdfs_path}")
            
            # Upload
            success, message = self.upload_file_to_hdfs(file_info['path'], hdfs_path)
            
            if success:
                print(f"  Succès!")
                upload_summary['success'] += 1
            else:
                print(f"  Échec: {message}")
                upload_summary['failed'] += 1
            
            upload_summary['details'].append({
                'filename': filename,
                'local_path': file_info['path'],
                'hdfs_path': hdfs_path,
                'category': file_info['category'],
                'size': file_info['size'],
                'success': success,
                'message': message
            })
        
        return upload_summary
    
    def create_optimized_metadata(self, upload_summary, found_files):
        """
        Crée des métadonnées optimisées
        """
        print("\n=== Création des métadonnées ===")
        
        # Analyse des fichiers uploadés
        content_analysis = {}
        for filename, file_info in found_files.items():
            try:
                with open(file_info['path'], 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if filename == 'sentiment_analysis.json':
                    content_analysis[filename] = {
                        'type': 'sentiment_analysis',
                        'total_days': data.get('total_days', 0),
                        'methodology': data.get('methodology', ''),
                        'first_result_keys': list(data.get('results', {}).keys())[:5]
                    }
                elif filename == 'geographic_analysis.json':
                    content_analysis[filename] = {
                        'type': 'geographic_analysis', 
                        'total_countries': data.get('total_countries', 0),
                        'total_cities': data.get('total_cities', 0),
                        'methodology': data.get('methodology', '')
                    }
                elif filename == 'tweets_with_locations.json':
                    content_analysis[filename] = {
                        'type': 'organized_tweets',
                        'total_records': len(data) if isinstance(data, list) else 0,
                        'sample_fields': list(data[0].keys()) if data and isinstance(data, list) else []
                    }
                    
            except Exception as e:
                content_analysis[filename] = {'error': str(e)}
        
        metadata = {
            'upload_info': {
                'timestamp': datetime.now().isoformat(),
                'hdfs_base_path': self.hdfs_base_path,
                'uploader_version': 'optimized_v2.0_with_structure_creation'
            },
            'summary': {
                'total_files': upload_summary['total'],
                'successful_uploads': upload_summary['success'],
                'failed_uploads': upload_summary['failed'],
                'success_rate': f"{(upload_summary['success']/upload_summary['total']*100):.1f}%"
            },
            'files': upload_summary['details'],
            'content_analysis': content_analysis,
            'hdfs_structure': {
                'base_path': self.hdfs_base_path,
                'mapreduce_results': {
                    'sentiment': 'Résultats de l\'analyse de sentiment par jour',
                    'geographic': 'Analyse de distribution géographique des tweets',
                    'organized_data': 'Données organisées des tweets avec localisation'
                }
            }
        }
        
        # Sauvegarder les métadonnées
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        metadata_file = f"mapreduce_upload_metadata_{timestamp}.json"
        
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"Métadonnées créées: {metadata_file}")
        
        # Upload vers HDFS
        hdfs_metadata_path = f"{self.hdfs_base_path}/metadata/{metadata_file}"
        success, message = self.upload_file_to_hdfs(metadata_file, hdfs_metadata_path)
        
        if success:
            print(f"Métadonnées uploadées vers HDFS")
        else:
            print(f"Échec upload métadonnées: {message}")
        
        return metadata
    
    def verify_uploads(self, upload_summary):
        """
        Vérifie que les fichiers sont bien dans HDFS
        """
        print("\n=== Vérification des uploads ===")
        
        verified_count = 0
        
        for detail in upload_summary['details']:
            if not detail['success']:
                continue
            
            hdfs_path = detail['hdfs_path']
            
            try:
                if self.use_docker:
                    cmd = ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-test', '-e', hdfs_path]
                else:
                    cmd = ['hdfs', 'dfs', '-test', '-e', hdfs_path]
                
                result = subprocess.run(cmd, capture_output=True, timeout=30)
                
                if result.returncode == 0:
                    verified_count += 1
                    print(f"  {detail['filename']}")
                else:
                    print(f"  {detail['filename']} non trouvé")
                    
            except Exception as e:
                print(f"  Erreur vérification {detail['filename']}: {e}")
        
        print(f"\nVérification: {verified_count}/{upload_summary['success']} fichiers confirmés")
        return verified_count
    
    def display_final_structure(self):
        """
        Affiche la structure HDFS finale
        """
        print("\n=== Structure HDFS créée ===")
        
        try:
            if self.use_docker:
                cmd = ['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '-R', f"{self.hdfs_base_path}/mapreduce_results"]
            else:
                cmd = ['hdfs', 'dfs', '-ls', '-R', f"{self.hdfs_base_path}/mapreduce_results"]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    if line.strip():
                        if line.startswith('drwx'):
                            print(f"{line.split()[-1]}")
                        elif line.startswith('-rw'):
                            parts = line.split()
                            size = parts[4]
                            name = parts[-1]
                            print(f"{name} ({size} bytes)")
            else:
                print(f"Erreur affichage: {result.stderr}")
                
        except Exception as e:
            print(f"Exception: {e}")

def main():
    """
    Fonction principale optimisée avec création automatique de structure
    """
    print("=== UPLOAD OPTIMISÉ DES RÉSULTATS MAPREDUCE VERS HDFS ===")
    print(f"Début: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialiser l'uploader
    uploader = OptimizedHDFSUploader()
    
    # 1. Vérifier HDFS
    if not uploader.check_hdfs_connectivity():
        print("HDFS non accessible. Vérifiez votre configuration.")
        return False
    
    # 2. Créer la structure HDFS complète AVANT de chercher les fichiers
    print("\n=== PRÉPARATION DE L'ENVIRONNEMENT HDFS ===")
    if not uploader.create_complete_hdfs_structure():
        print("Impossible de créer la structure HDFS nécessaire.")
        return False
    
    # 3. Tester l'accès en écriture
    if not uploader.test_hdfs_write_access():
        print("Problème d'accès en écriture HDFS.")
        return False
    
    # 4. Trouver les fichiers cibles
    found_files = uploader.find_target_files()
    if not found_files:
        print("Fichiers requis manquants. Vérifiez la génération des résultats MapReduce.")
        return False
    
    # 5. Confirmation
    print(f"\nPrêt à uploader {len(found_files)} fichiers d'analyse MapReduce")
    print("   • sentiment_analysis.json -> Analyse des sentiments par jour")
    print("   • geographic_analysis.json -> Distribution géographique")  
    print("   • tweets_with_locations.json -> Données organisées")
    
    response = input("\nContinuer l'upload? (y/N): ").strip().lower()
    if response not in ['y', 'yes', 'oui']:
        print("Upload annulé")
        return False
    
    # 6. Upload des fichiers (structure déjà créée)
    upload_summary = uploader.upload_target_files(found_files)
    
    # 7. Créer les métadonnées
    metadata = uploader.create_optimized_metadata(upload_summary, found_files)
    
    # 8. Vérification
    verified_count = uploader.verify_uploads(upload_summary)
    
    # 9. Affichage final
    uploader.display_final_structure()
    
    # 10. Résumé
    print(f"\n=== RÉSUMÉ FINAL ===")
    print(f"Fichiers traités: {upload_summary['total']}")
    print(f"Uploads réussis: {upload_summary['success']}")
    print(f"Échecs: {upload_summary['failed']}")
    print(f"Vérifiés: {verified_count}")
    print(f"Taux de succès: {metadata['summary']['success_rate']}")
    
    if upload_summary['success'] == 3:
        print("Tous les fichiers d'analyse MapReduce ont été uploadés avec succès!")
        print(f"Accès HDFS: {uploader.hdfs_base_path}/mapreduce_results/")
    
    print(f"Fin: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return upload_summary['failed'] == 0

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)