import psycopg2  # pip install psycopg2
import csv

# Database connection parameters
DB_HOST = 'localhost'
DB_NAME = 'companiesdb'
DB_USER = 'postgres'
DB_PASSWORD = '1'
DB_PORT = '5432'


# Function to connect to postgreSQL
def connect_to_db():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )


# Main function to ingest data
def ingest_data():
    # connect to postgreSQL
    conn = connect_to_db()
    cur = conn.cursor()

    # Open the csv file
    with open('companies.csv', 'r') as file:
        data_reader = csv.reader(file)
        next(data_reader)  # Skip the header row

        # Insert each row into the table
        for row in data_reader:
            # 去掉逗号，处理 Revenue 和 Employees 列
            row[3] = row[3].replace(',', '')  # 处理 Revenue_USD_millions 列的逗号
            row[5] = row[5].replace(',', '')  # 处理 Employees 列的逗号
            # 插入数据到表中，所有列使用 %s 作为占位符
            cur.execute("INSERT INTO companies (Rank,Name,Industry,Revenue_USD_millions,Revenue_growth,Employees,"
                        "Headquarters) VALUES (%s,%s,%s,%s,%s,%s,%s)", row)

    # Commit and close the connection
    conn.commit()
    cur.close()
    conn.close()
    print("Data ingested successfully")


if __name__ == "__main__":
    ingest_data()
