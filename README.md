# DB Backups

This is a simple python script that backs up your MySQL databases. You define as many DBs that you want in the config.yml file and at the top of every hour, backups will be generated in the "dbs" directory.

To restore any of these backups, simply import the sql file into PHPMyAdmin and it'll handle the rest!