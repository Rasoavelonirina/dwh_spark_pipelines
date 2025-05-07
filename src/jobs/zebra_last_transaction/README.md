---

## Job : zebra_last_transaction

### Objectif

Ce job calcule, pour chaque client ZEBRA identifié comme **ACTIF**, la date et la catégorie de sa **dernière transaction enregistrée**. Il prend en compte différents canaux de transaction pour déterminer quelle partie (expéditeur ou destinataire) est considérée comme le client effectuant l'action pertinente.

### Logique Métier Principale

1.  **Identification des Clients Actifs:** Le job lit la table master des utilisateurs (`DWH.zebra_rp2p_master` par défaut) et sélectionne les `msisdn` des utilisateurs dont le statut (`user_status`) correspond à la valeur configurée pour "actif" (ex: 'ACTIF').
2.  **Lecture des Transactions:** Il lit la table des transactions (`DWH.zebra_rp2p_transaction` par défaut) pour la ou les dates à traiter (déterminées par le mode d'exécution : `daily`, `initial`, `catchup`, `specific_date`).
3.  **Analyse par Canal (`tra_channel`) :** Pour chaque transaction dans la période :
    *   **Si `tra_channel` = 'C2S' (Customer to Seller/System):** L'expéditeur (`tra_sndr_msisdn`) est considéré comme le client pertinent. Sa catégorie associée (`tra_sndr_category`) et la date de transaction (`tra_date`) sont retenues pour ce client.
    *   **Si `tra_channel` = 'O2C' (Operator/Organization to Customer):** Le destinataire (`tra_receiver_msisdn`) est considéré comme le client pertinent. Sa catégorie associée (`tra_receiver_category`) et la date de transaction (`tra_date`) sont retenues pour ce client.
    *   **Si `tra_channel` = 'C2C' (Customer to Customer):** *Les deux* participants sont considérés comme ayant effectué une transaction.
        *   L'expéditeur (`tra_sndr_msisdn`) est retenu avec sa catégorie (`tra_sndr_category`) et la date.
        *   Le destinataire (`tra_receiver_msisdn`) est retenu avec sa catégorie (`tra_receiver_category`) et la date.
        *   Cela signifie qu'une seule transaction C2C peut potentiellement mettre à jour la "dernière transaction" de deux clients différents.
4.  **Filtrage par Clients Actifs:** Seuls les événements de transaction (étape 3) concernant un `msisdn` identifié comme actif (étape 1) sont conservés.
5.  **Détermination de la Dernière Transaction:** Pour chaque client actif ayant eu au moins une transaction pertinente dans la période analysée (ou depuis le début si run `initial`), le job identifie l'événement de transaction le plus récent (basé sur `tra_date`).
6.  **Construction de la Sortie:** Le job génère un enregistrement par client actif ayant eu une transaction, contenant :
    *   `party_id`: Le MSISDN du client.
    *   `zb_last_transaction_date`: La date de la dernière transaction pertinente identifiée.
    *   `zb_user_category`: La catégorie (expéditeur ou destinataire, selon le canal de la *dernière* transaction) associée à cette dernière transaction.
    *   `zb_user_status`: Le statut du client (sera 'ACTIF' car filtré).
    *   `day`: La date de référence du traitement (correspondant à la date la plus récente des données traitées, ex: J-1 pour le run `daily`). Cette colonne est utilisée pour le partitionnement.

### Configuration Spécifique (`config/jobs/zebra_last_transaction.yaml`)

Ce job nécessite la configuration suivante dans son fichier YAML :

*   **`data_sources`:**
    *   `zebra_users`: Doit pointer vers la connexion (`connection_name`) et la table (`table`) contenant les informations master des utilisateurs (MSISDN, statut, catégorie).
    *   `zebra_transactions`: Doit pointer vers la connexion (`connection_name`) et la table (`table`) contenant l'historique des transactions (MSISDN expéditeur/destinataire, date, catégories, canal).
*   **`columns`:** Mappings corrects des noms de colonnes logiques (`msisdn`, `status`, `category`, `sndr_msisdn`, `rcvr_msisdn`, `date`, `sndr_category`, `rcvr_category`, `channel`) vers les noms physiques dans les tables sources.
*   **`business_logic`:**
    *   `active_status_value`: Valeur exacte du statut actif (ex: 'ACTIF').
    *   `channel_c2s`, `channel_o2c`, `channel_c2c`: Valeurs exactes des identifiants de canaux.
*   **`outputs`:**
    *   `last_transaction_output`: Définit la destination des résultats.
        *   `output_type`: Doit être `parquet_file` (ou adapté si changé).
        *   `path`: Chemin de sortie HDFS/S3/local.
        *   `partition_by`: Doit inclure `["day"]`.
        *   `mode`: Typiquement `overwrite` (pour écraser les partitions existantes du jour traité).
*   **`metadata`:** Configuration correcte pour le suivi (`tracker_file_path` ou BDD) avec un `job_name_identifier` unique (`zebra_last_transaction`).

### Exécution

Utiliser le script `run_spark_job.sh` avec `--job-name zebra_last_transaction` et le mode d'exécution souhaité (`daily`, `initial`, `catchup`, `specific_date`).

*   **Mode `daily`:** Traite automatiquement les transactions de la veille (J-1) et rattrape les jours manqués depuis la dernière exécution réussie (basé sur les métadonnées).
*   **Mode `initial`:** Nécessite `--start-date` et `--end-date` pour traiter une période historique large. Calculera la dernière transaction sur toute cette période.
*   **Mode `catchup`:** Rattrape explicitement les jours manqués entre la dernière exécution réussie et J-1.
*   **Mode `specific_date`:** Traite uniquement les transactions de la date spécifiée avec `--processing-date`.

### Sortie

Le job écrit des fichiers Parquet dans le `path` de sortie configuré, partitionnés par la colonne `day` (au format YYYY-MM-DD). Chaque partition contient la photo de la dernière transaction connue pour les clients actifs *à la date du traitement*.

**Exemple de Structure de Sortie :**

```
/data/processed/zebra_last_transaction/
├── day=2024-03-14/
│   └── part-00000-....parquet
├── day=2024-03-15/
│   └── part-00000-....parquet
└── ...
```

### Dépendances Externes

*   Accès à la base de données (MariaDB par défaut) configurée pour `zebra_users` et `zebra_transactions`.
*   Driver JDBC MariaDB accessible par Spark.
*   Accès en écriture au chemin de sortie et au chemin/table de métadonnées.

---