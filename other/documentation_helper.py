from rich.console import Console
from rich.table import Table

def main():
    table_0 = Table(title="PostgreSQL Applications Table")
    table_0.add_column("Field", justify="left", no_wrap=True)
    table_0.add_column("SQL Type", justify="left", no_wrap=True)
    table_0.add_column("Python Type", justify="left", no_wrap=True)
    table_0.add_row("application_id*", "SERIAL PRIMARY KEY", "int")
    table_0.add_row("posted_at", "TIMESTAMP", "datetime")
    table_0.add_row("job_board", "TEXT", "str")
    table_0.add_row("work_type", "TEXT", "str")
    table_0.add_row("position", "TEXT", "str")
    table_0.add_row("location", "TEXT", "str")
    table_0.add_row("company", "TEXT", "str")
    table_0.add_row("pay", "TEXT", "str")

    table_1 = Table(title="PostgreSQL Handshake Meta Table")
    table_1.add_column("Field", justify="left", no_wrap=True)
    table_1.add_column("SQL Type", justify="left", no_wrap=True)
    table_1.add_column("Python Type", justify="left", no_wrap=True)
    table_1.add_row("(application_id)", "REFERENCE", "int")
    table_1.add_row("expires_at", "TIMESTAMP", "datetime")
    table_1.add_row("contract_start", "TIMESTAMP", "datetime")
    table_1.add_row("contract_end", "TIMESTAMP", "datetime")
    table_1.add_row("handshake_id", "INT UNIQUE ", "int")
    table_1.add_row("docs_required", "TEXT[]", "list\\[str]")
    table_1.add_row("on_handshake", "BOOLEAN", "bool")
    table_1.add_row("industry_type", "TEXT", "str")

    table_2 = Table(title="Milvus (Vector Database) Applications Table")
    table_2.add_column("Field", justify="left", no_wrap=True)
    table_2.add_column("Milvus Type", justify="left", no_wrap=True)
    table_2.add_column("Python Type", justify="left", no_wrap=True)
    table_2.add_row("milvus_id*", "SERIAL PRIMARY KEY", "int")
    table_2.add_row("(application_id)", "REFERENCE", "int")
    table_2.add_row("idx_chunk", "INT", "int")
    table_2.add_row("txt_chunk", "VARCHAR(chunk_size)", "str")
    table_2.add_row("vec_chunk", "FLOAT_VECTOR", "list\\[float]")


    with open('documentation_helper.out', "w") as f:
        console = Console(file=f)
        console.print(table_0)
        f.write("\n")
        console.print(table_1)
        f.write("\n")
        console.print(table_2)    
    

if __name__ == "__main__":
    main()