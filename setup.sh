#!/bin/bash

# Create a Conda environment
conda create -y -n myenv python=3.9

# Activate the Conda environment
conda activate myenv

# Install the required dependencies
pip install -r requirements.txt

# Create the 'data' directory
mkdir data

# Change directory to 'data'
cd data

# Create the 'ntd.db' SQLite database
touch ntd.db

# Print setup completed message
echo "Setup completed successfully. You can now run the main.py file."
