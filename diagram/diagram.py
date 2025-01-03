from diagrams import Diagram, Cluster, Edge
from diagrams.aws.analytics import EMR
from diagrams.aws.storage import S3
from diagrams.aws.management import Cloudwatch
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.analytics import Dbt
from diagrams.onprem.client import Client
from diagrams.generic.database import SQL
from diagrams.generic.compute import Rack

with Diagram("DBT-Spark with Medallion Architecture (External Sources)", show=False, direction="TB"):
    # External Sources Cluster
    with Cluster("External Sources"):
        generic_source = Rack("Generic Data Source")
        sql_source = SQL("SQL Database")
        client_source = Client("Client Files (CSV/Excel)")

    # S3 Layers
    with Cluster("S3 Storage (Data Lake)"):
        bronze = S3("Bronze (Raw Data)")
        silver = S3("Silver (Clean Data)")
        gold = S3("Gold (Aggregated Data)")

    # Orchestration
    orchestrator = Airflow("Airflow Orchestration")

    # Processing Cluster
    with Cluster("Data Processing"):
        spark_emr = EMR("Spark on EMR")
        dbt_spark = Dbt("DBT on Spark")

    # Monitoring
    monitoring = Cloudwatch("CloudWatch Metrics & Alerts")

    # Data Flow
    generic_source >> Edge(label="Ingest Raw Data") >> bronze
    sql_source >> Edge(label="Ingest Raw Data") >> bronze
    client_source >> Edge(label="Ingest Raw Data") >> bronze
    orchestrator >> Edge(label="Trigger Pipeline") >> spark_emr
    spark_emr >> Edge(label="Write Raw Data") >> bronze
    spark_emr >> Edge(label="Transform & Clean Data") >> silver
    spark_emr >> Edge(label="Validate Data Quality") >> monitoring
    dbt_spark >> Edge(label="Enrich & Aggregate Data") >> gold

    # Monitoring Flow
    silver >> Edge(label="Send Data Quality Metrics") >> monitoring
    gold >> Edge(label="Send Test Results") >> monitoring