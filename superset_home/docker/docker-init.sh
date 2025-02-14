#!/bin/bash
set -e

echo "Initializing Superset..."

# Mise à jour de la base de données
superset db upgrade

# Création de l'administrateur
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password admin

# Initialisation
superset init

echo "Superset initialized successfully!"