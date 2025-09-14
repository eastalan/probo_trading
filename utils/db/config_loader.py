#!/usr/bin/env python3
"""
Configuration loader for database and application settings
"""
import yaml
import pymysql
import os

class ConfigLoader:
    def __init__(self, config_path="config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self):
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML configuration: {e}")
    
    def get_db_config(self):
        """Get database configuration for PyMySQL"""
        db_config = self.config['database']['mysql']
        return {
            'host': db_config['host'],
            'user': db_config['user'],
            'password': db_config['password'],
            'database': db_config['database'],
            'charset': db_config['charset'],
            'cursorclass': pymysql.cursors.DictCursor,
            'autocommit': db_config.get('autocommit', True),
            'ssl_disabled': db_config.get('ssl_disabled', True)
        }
    
    def get_fotmob_config(self):
        """Get FotMob configuration"""
        return self.config['fotmob']
    
    def get_melbet_config(self):
        """Get Melbet configuration"""
        return self.config['melbet']
    
    def get_app_config(self):
        """Get application configuration"""
        return self.config['app']
    
    def get_db_connection(self):
        """Get a database connection"""
        return pymysql.connect(**self.get_db_config())

# Global config instance
config = ConfigLoader()

# Convenience functions
def get_db_config():
    return config.get_db_config()

def get_db_connection():
    return config.get_db_connection()

def get_fotmob_config():
    return config.get_fotmob_config()

def get_melbet_config():
    return config.get_melbet_config()

def get_app_config():
    return config.get_app_config()
