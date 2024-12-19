from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mysql.connector
from typing import List, Optional
import os
from datetime import datetime

app = FastAPI()

# Database connection configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "dummy_user"),
    "password": os.getenv("DB_PASSWORD", "dummy_password"),
    "database": os.getenv("DB_NAME", "dummy_db")
}

# Helper function to get database connection
def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as err:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(err)}")

# Base models for request validation
class TableCreate(BaseModel):
    table_name: str
    columns: dict  # Format: {"column_name": "column_type"}

class RowData(BaseModel):
    data: dict  # Format: {"column_name": "value"}

# Table Operations
@app.post("/tables/create")
async def create_table(table_info: TableCreate):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Construct CREATE TABLE query
        columns = [f"{name} {type_}" for name, type_ in table_info.columns.items()]
        query = f"""CREATE TABLE {table_info.table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {', '.join(columns)},
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
        
        cursor.execute(query)
        conn.commit()
        return {"message": f"Table {table_info.table_name} created successfully"}
    
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@app.get("/tables")
async def list_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        return {"tables": tables}
    
    finally:
        cursor.close()
        conn.close()

@app.delete("/tables/{table_name}")
async def delete_table(table_name: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"DROP TABLE {table_name}")
        conn.commit()
        return {"message": f"Table {table_name} deleted successfully"}
    
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

# Row Operations
@app.post("/tables/{table_name}/rows")
async def insert_row(table_name: str, row_data: RowData):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        columns = ', '.join(row_data.data.keys())
        values = ', '.join([f"'{value}'" for value in row_data.data.values()])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        
        cursor.execute(query)
        conn.commit()
        return {"message": "Row inserted successfully", "id": cursor.lastrowid}
    
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@app.get("/tables/{table_name}/rows")
async def get_rows(table_name: str, limit: int = 100, offset: int = 0):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}")
        rows = cursor.fetchall()
        return {"rows": rows}
    
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@app.put("/tables/{table_name}/rows/{row_id}")
async def update_row(table_name: str, row_id: int, row_data: RowData):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        updates = ', '.join([f"{key} = '{value}'" for key, value in row_data.data.items()])
        query = f"UPDATE {table_name} SET {updates} WHERE id = {row_id}"
        
        cursor.execute(query)
        conn.commit()
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Row not found")
            
        return {"message": f"Row {row_id} updated successfully"}
    
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

@app.delete("/tables/{table_name}/rows/{row_id}")
async def delete_row(table_name: str, row_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"DELETE FROM {table_name} WHERE id = {row_id}")
        conn.commit()
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Row not found")
            
        return {"message": f"Row {row_id} deleted successfully"}
    
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

# Table Schema Operations
@app.get("/tables/{table_name}/schema")
async def get_table_schema(table_name: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        schema = [{"Field": col[0], "Type": col[1], "Null": col[2], "Key": col[3], "Default": col[4]} for col in columns]
        return {"schema": schema}
    
    except mysql.connector.Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)