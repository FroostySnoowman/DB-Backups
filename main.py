import aiomysql
import asyncio
import yaml
import os
from datetime import datetime, timedelta

def generate_header():
    return f"""-- MySQL Export

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";
"""

async def export_databases(config, output_path):
    now = datetime.now()

    date_folder_name = now.strftime("%m-%d-%y")
    hour_folder_name = now.strftime("%m-%d-%y %H:00")

    date_folder_path = os.path.join(output_path, date_folder_name)
    os.makedirs(date_folder_path, exist_ok=True)

    hour_folder_path = os.path.join(date_folder_path, hour_folder_name)
    os.makedirs(hour_folder_path, exist_ok=True)

    counter = 1

    for db in config['DBS']:
        db_structure = f"DB_{counter}"
        async with aiomysql.connect(host=db[db_structure]['Host'], port=db[db_structure]['Port'], user=db[db_structure]['Username'], password=db[db_structure]['Password'], db=db[db_structure]['Database']) as conn:
            async with conn.cursor() as cur:

                dump_content = generate_header()

                await cur.execute('SHOW TABLES')
                tables = await cur.fetchall()
                for table in tables:
                    await cur.execute(f"SHOW CREATE TABLE {table[0]}")
                    create_table = await cur.fetchone()
                    dump_content += f"\n-- Table structure for table `{table[0]}`\n--\n{create_table[1]};\n"

                    await cur.execute(f'SELECT * FROM {table[0]}')
                    rows = await cur.fetchall()
                    if rows:
                        dump_content += f"\n-- Dumping data for table `{table[0]}`\n--\n"
                        for row in rows:
                            row_values = ','.join(map(lambda x: f"'{x}'", row))
                            dump_content += f"INSERT INTO {table[0]} VALUES ({row_values});\n"

                dump_content += "\nCOMMIT;"

                with open(f"{hour_folder_path}/{db[db_structure]['Database']}.sql", 'w') as f:
                    f.write(dump_content)
        counter += 1

async def delete_old_backups(config, output_path):
    now = datetime.now()
    for root, dirs, files in os.walk(output_path):
        for directory in dirs:
            dir_parts = directory.split(' ')
            if len(dir_parts) >= 2:
                date_str = dir_parts[0]
                hour_str = dir_parts[1].split(':')[0]
                try:
                    folder_date = datetime.strptime(date_str, "%m-%d-%y")
                    folder_date_with_hour = folder_date.replace(hour=int(hour_str), minute=0)
                    if (now - folder_date_with_hour) > timedelta(days=config["Delete_Backups_After_Days"]):
                        folder_path = os.path.join(root, directory)
                        for file in os.listdir(folder_path):
                            file_path = os.path.join(folder_path, file)
                            os.remove(file_path)
                        if not os.listdir(folder_path):
                            os.rmdir(folder_path)
                except ValueError as e:
                    print(f"Error processing directory: {directory}. {e}")

async def main():
    with open('config.yml', 'r') as stream:
        try:
            config = yaml.safe_load(stream)
            output_directory = "dbs"
            await export_databases(config, output_directory)
            await delete_old_backups(config, output_directory)
            now = datetime.now()
            time = now.strftime("%m-%d-%y %H")
            print(f"{time} - Export complete.")
        except yaml.YAMLError as exc:
            print(exc)

async def run_at_top_of_hour():
    while True:
        now = datetime.now()
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        seconds_until_next_hour = (next_hour - now).total_seconds()
        await asyncio.sleep(seconds_until_next_hour)
        await main()

if __name__ == "__main__":
    print("Script has started running!")
    asyncio.run(run_at_top_of_hour())
