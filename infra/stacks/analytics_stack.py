from pathlib import Path

from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_glue as glue
from aws_cdk import aws_s3 as s3
from constructs import Construct


class AvatureEtlAnalyticsStack(Stack):
    """
    Analytics layer:
      - Glue Data Catalog database
      - Athena workgroup
      - Athena named queries backed by external SQL files

    Notes:
      - Named queries are saved in Athena, but they are NOT executed automatically.
      - Glue crawlers are intentionally omitted because the schema and S3 layout are stable.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        stage: str,
        outputs_bucket: s3.IBucket,
        dataset_root: str = "avature",
        athena_bytes_scanned_cutoff_mb: int = 512,
        **kwargs,
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            description=f"Avature ETL Analytics Stack [{stage}]",
            **kwargs,
        )

        database_name = f"{prefix.replace('-', '_')}_{stage}_analytics"
        workgroup_name = f"{prefix}-{stage}-athena"

        self.database_name = database_name
        self.workgroup_name = workgroup_name

        bucket_name = outputs_bucket.bucket_name
        athena_results_prefix = f"{dataset_root}/athena-results/"

        sql_dir = Path(__file__).resolve().parents[1] / "sql"

        glue_db = glue.CfnDatabase(
            self,
            "AnalyticsDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=database_name,
                description="Analytics database for Avature ETL bronze/silver/gold and ops datasets",
            ),
        )

        workgroup = athena.CfnWorkGroup(
            self,
            "AthenaWorkGroup",
            name=workgroup_name,
            description="Athena workgroup for Avature ETL analytics",
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                # if a workgroup enforces a centralized query results location,
                # a CTAS query that specifies external_location fails
                enforce_work_group_configuration=False,
                publish_cloud_watch_metrics_enabled=False,
                bytes_scanned_cutoff_per_query=athena_bytes_scanned_cutoff_mb * 1024 * 1024,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{bucket_name}/{athena_results_prefix}"
                ),
            ),
        )
        workgroup.add_dependency(glue_db)

        # use partition projection (faster in-memory ops) to avoid needing to add partitions for each run date
        # the queries will filter by run_date to only read relevant data
        # https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html
        queries = [
            ("BronzeJobsRaw", "01_bronze_jobs_raw.sql", f"{prefix}-{stage}-01-bronze-jobs-raw"),
            ("OpsPortalSummaryRaw", "02_ops_portal_summary_raw.sql", f"{prefix}-{stage}-02-ops-portal-summary-raw"),
            (
                "SilverJobsCuratedCtas",
                "03_silver_jobs_curated_ctas.sql",
                f"{prefix}-{stage}-03-silver-jobs-curated-ctas",
            ),
            (
                "SilverJobsIncrementalInsert",
                "04_silver_jobs_incremental_insert.sql",
                f"{prefix}-{stage}-04-silver-jobs-incremental-insert",
            ),
            (
                "GoldPortalDailySummary",
                "05_gold_portal_daily_summary.sql",
                f"{prefix}-{stage}-05-gold-portal-daily-summary",
            ),
        ]

        for construct_id, filename, query_name in queries:
            query_sql = self._load_sql_template(
                sql_dir / filename,
                database_name=database_name,
                bucket_name=bucket_name,
                dataset_root=dataset_root,
            )

            named_query = athena.CfnNamedQuery(
                self,
                f"{construct_id}NamedQuery",
                database=database_name,
                name=query_name,
                query_string=query_sql,
                work_group=workgroup_name,
            )
            named_query.add_dependency(glue_db)
            named_query.add_dependency(workgroup)

        CfnOutput(self, "AnalyticsDatabaseName", value=database_name)
        CfnOutput(self, "AthenaWorkGroupName", value=workgroup_name)
        CfnOutput(self, "AthenaResultsLocation", value=f"s3://{bucket_name}/{athena_results_prefix}")
        CfnOutput(self, "AthenaBytesScannedCutoffMb", value=str(athena_bytes_scanned_cutoff_mb))

    @staticmethod
    def _load_sql_template(
        path: Path,
        *,
        database_name: str,
        bucket_name: str,
        dataset_root: str,
        run_date_filter: str = "CAST(current_date AS varchar)",
    ) -> str:
        sql = path.read_text(encoding="utf-8")
        return (
            sql.replace("__DATABASE_NAME__", database_name)
            .replace("__BUCKET_NAME__", bucket_name)
            .replace("__DATASET_ROOT__", dataset_root)
            .replace("__RUN_DATE_FILTER__", run_date_filter)
        )
