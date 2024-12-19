# main.py
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI()

# Database configuration - use environment variables in production
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "dummy_user"),
    "password": os.getenv("DB_PASSWORD", "dummy_password"),
    "database": os.getenv("DB_NAME", "dummy_db")
}

# Models
class TableSchema(BaseModel):
    table_name: str
    columns: dict  # Format: {"column_name": "column_type"}

class RowData(BaseModel):
    table_name: str
    data: dict    # Format: {"column_name": "value"}

class UpdateData(BaseModel):
    table_name: str
    data: dict    # New values
    where: dict   # Condition

# Database connection helper
def get_db():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True)
        yield cursor
        connection.commit()
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

# Table Operations
@app.post("/table/create")
async def create_table(schema: TableSchema, cursor = Depends(get_db)):
    columns = ", ".join([f"{k} {v}" for k, v in schema.columns.items()])
    query = f"CREATE TABLE {schema.table_name} ({columns})"
    try:
        cursor.execute(query)
        return {"message": f"Table {schema.table_name} created successfully"}
    except Error as e:
        raise HTTPException(status_code=400, detail=str(e))

# @app.get("/tables")
# async def get_tables(cursor = Depends(get_db)):
#     cursor.execute("SHOW TABLES")
#     tables = [table[0] for table in cursor.fetchall()]
#     return {"tables": tables}

@app.get("/tables")
async def get_tables(cursor = Depends(get_db)):
    cursor.execute("SHOW TABLES")
    tables = [table['Tables_in_' + DB_CONFIG['database']] for table in cursor.fetchall()]
    return {"tables": tables}

@app.delete("/table/{table_name}")
async def delete_table(table_name: str, cursor = Depends(get_db)):
    try:
        cursor.execute(f"DROP TABLE {table_name}")
        return {"message": f"Table {table_name} deleted successfully"}
    except Error as e:
        raise HTTPException(status_code=400, detail=str(e))

# Row Operations
@app.post("/row/insert")
async def insert_row(row_data: RowData, cursor = Depends(get_db)):
    columns = ", ".join(row_data.data.keys())
    values = ", ".join(["%s"] * len(row_data.data))
    query = f"INSERT INTO {row_data.table_name} ({columns}) VALUES ({values})"
    try:
        cursor.execute(query, list(row_data.data.values()))
        return {"message": "Row inserted successfully"}
    except Error as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/table/{table_name}/rows")
async def get_rows(table_name: str, cursor = Depends(get_db)):
    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        return {"rows": rows}
    except Error as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.put("/row/update")
async def update_row(update_data: UpdateData, cursor = Depends(get_db)):
    set_clause = ", ".join([f"{k} = %s" for k in update_data.data.keys()])
    where_clause = " AND ".join([f"{k} = %s" for k in update_data.where.keys()])
    query = f"UPDATE {update_data.table_name} SET {set_clause} WHERE {where_clause}"
    
    try:
        values = list(update_data.data.values()) + list(update_data.where.values())
        cursor.execute(query, values)
        return {"message": "Row updated successfully"}
    except Error as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/row/delete")
async def delete_row(table_name: str, where: dict, cursor = Depends(get_db)):
    where_clause = " AND ".join([f"{k} = %s" for k in where.keys()])
    query = f"DELETE FROM {table_name} WHERE {where_clause}"
    try:
        cursor.execute(query, list(where.values()))
        return {"message": "Row deleted successfully"}
    except Error as e:
        raise HTTPException(status_code=400, detail=str(e))