import os
import logging

import asyncpg
import numpy as np
import pandas as pd


logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class GeneralUtils:
    def __init__(self):
        self.db_docgenius: str = os.getenv('DB_DOCGENIUS') # docgenius database url

    async def get_db_connection(self, db_name: str):
        """
        Select the database URL based on the provided database name.
        Establish a connection to the database using the provided database URL.
        
        Parameters
        ----------
        db_name : str
            The name of the database to connect to.
        
        Returns
        -------
        conn : asyncpg.Connection
            The connection object to the specified database.
        """
        try:
            if db_name == 'db_docgenius':
                db_url=self.db_docgenius
            else:
                raise ValueError(f"Failed to connect to the database: Database '{db_name}' not recognized")
            
            conn = await asyncpg.connect(db_url)
            return conn
        
        except ValueError:
            raise  # Re-raise the exception

        except Exception as e:
            raise ValueError(f"Failed to connect to the database: {e}")
    

    async def docgenius_append_to_db(self, db_name:str, df: pd.DataFrame, table_name: str, schema_name: str = 'public', conn=None):
        """
        Append DataFrame into a database table.
        This function checks if the DataFrame columns match the table columns in the database.
        If they do, it appends the DataFrame to the table. If not, it raises a ValueError.
        If the connection is not provided, it will create a new one and close it after the operation.

        Parameters
        ----------
        db_name : str
            The name of the database to connect to.
        df : pd.DataFrame
            The DataFrame to be appended to the database table.
        table_name : str
            The name of the table in the database where the DataFrame will be appended.
        schema_name : str, optional
            The schema name of the table in the database (default is 'public').
        conn : asyncpg.Connection, optional
            The connection object to the database (default is None). If None, a new connection will be created.
        """

        close_conn = False
        if conn is None:
            conn = await self.get_db_connection(db_name)
            close_conn = True
        
        try:
            query_columns = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = $1 AND table_name = $2
            """
            table_columns = await conn.fetch(query_columns, schema_name, table_name)
            table_columns = [col['column_name'] for col in table_columns]

            valid_columns = [col for col in df.columns if col in table_columns]            
            if not valid_columns:
                raise ValueError(f"No matching columns found between DataFrame and table '{schema_name}.{table_name}'")
            
            if len(valid_columns) != len(df.columns):
                logging.warning(f"DataFrame columns not matching with table '{schema_name}.{table_name}'.")
                columns_to_drop = [col for col in df.columns if col not in valid_columns]
                logging.warning(f"Dropping columns: {columns_to_drop}")
            
            filtered_df = df[valid_columns].copy()

            if "timestamp" in filtered_df.columns:
                filtered_df["timestamp"] = filtered_df["timestamp"].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)
            
            filtered_df = filtered_df.replace({np.nan: None})
            data = [tuple(None if value == 'None' else value for value in row) for row in filtered_df.itertuples(index=False, name=None)]
            
            columns = ', '.join(f'"{col}"' for col in filtered_df.columns)

            values = ', '.join(f'${i + 1}' for i in range(len(filtered_df.columns)))
            query_insert = f'INSERT INTO "{schema_name}"."{table_name}" ({columns}) VALUES ({values})'
            await conn.executemany(query_insert, data)
            
            logging.info(f"Data ready to appended into '{schema_name}.{table_name}'")
            return True

        except Exception as e: 
            logging.error(f"Error while inserting data into '{table_name}': {e}")
            raise ValueError(f"Error while inserting data into '{table_name}': {e}")
        
        finally: 
            if close_conn and 'conn' in locals(): 
                await conn.close()
    

    async def get_last_date_docgenius(self) -> pd.DataFrame:
        """
        Get the last update date from the docgenius database.

        Returns
        -------
        df : pd.DataFrame
            A DataFrame containing the last update date from the docgenius database.

        """
        db_name = 'db_docgenius'
        conn = await self.get_db_connection(db_name)
        try:
            query = f'SELECT timestamp FROM "logs"."req_logs"'
            result = await conn.fetch(query)
            df = pd.DataFrame([dict(record) for record in result])
            return df

        except Exception as e:
            raise ValueError(f'Query failed for "logs"."req_logs": {e}')
