# Databricks notebook source
from datetime import datetime

def log_info(message):

    print(
        f"""
        [INFO]
        {datetime.now()}
        {message}
        """
    )

def log_error(message):

    print(
        f"""
        [ERROR]
        {datetime.now()}
        {message}
        """
    )
