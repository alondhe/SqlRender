package org.ohdsi.sql;

import java.sql.SQLException;

public class TestSqlRender {

	public static void main(String[] args) throws SQLException {
		
		String sql = "delete from @results_schema.heracles_results where cohort_definition_id IN (@cohort_definition_id) and analysis_id IN (@list_of_analysis_ids);\r\n" + 
				"delete from @results_schema.heracles_results_dist where cohort_definition_id IN (@cohort_definition_id) and analysis_id IN (@list_of_analysis_ids);\r\n" + 
				"\r\n" + 
				"--7. generate results for analysis_results\r\n" + 
				"\r\n" + 
				"select subject_id, cohort_definition_id, cohort_start_date, cohort_end_date\r\n" + 
				"into #HERACLES_cohort\r\n" + 
				"from @results_schema.cohort\r\n" + 
				"where cohort_definition_id in (@cohort_definition_id)\r\n" + 
				";\r\n" + 
				"create index ix_cohort_subject on #HERACLES_cohort (subject_id, cohort_start_date);\r\n" + 
				"\r\n" + 
				"select distinct subject_id, cohort_definition_id\r\n" + 
				"into #HERACLES_cohort_subject\r\n" + 
				"from #HERACLES_cohort\r\n" + 
				";\r\n" + 
				"create index ix_cohort_subject_subject on #HERACLES_cohort_subject (subject_id, cohort_definition_id);\r\n" + 
				"\r\n" + 
				"select subject_id, cohort_definition_id, cohort_start_date, cohort_end_date\r\n" + 
				"into #cohort_first\r\n" + 
				"from (\r\n" + 
				"select subject_id, cohort_definition_id, cohort_start_date, cohort_end_date, row_number() over (partition by cohort_definition_id, subject_id order by cohort_start_date) as rn\r\n" + 
				"FROM #HERACLES_cohort\r\n" + 
				") F\r\n" + 
				"where F.rn = 1\r\n" + 
				";\r\n" + 
				"\r\n" + 
				"create index ix_cohort_first_subject on #cohort_first (subject_id, cohort_start_date);\r\n" + 
				"\r\n" + 
				"SELECT hp.period_id, hp.period_start_date, hp.period_end_date\r\n" + 
				"into #periods_baseline\r\n" + 
				"FROM @results_schema.heracles_periods hp\r\n" + 
				"WHERE not (\r\n" + 
				"hp.period_end_date <= (SELECT dateadd(d, -365, min(cohort_start_date)) FROM #HERACLES_cohort) or hp.period_start_date > (SELECT max(cohort_start_date) FROM #HERACLES_cohort)\r\n" + 
				") AND hp.period_type in (@periods); -- only returns overlapping periods\r\n" + 
				"\r\n" + 
				"create index ix_periods_baseline_start on #periods_baseline (period_start_date);\r\n" + 
				"create index ix_periods_baseline_end on #periods_baseline (period_end_date);\r\n" + 
				"\r\n" + 
				"SELECT hp.period_id, hp.period_start_date, hp.period_end_date\r\n" + 
				"into #periods_atrisk\r\n" + 
				"FROM @results_schema.heracles_periods hp\r\n" + 
				"WHERE not (\r\n" + 
				"hp.period_end_date <= (SELECT min(cohort_start_date) FROM #HERACLES_cohort) or hp.period_start_date > (SELECT max(cohort_end_date) FROM #HERACLES_cohort)\r\n" + 
				") AND hp.period_type in (@periods); -- only returns overlapping periods\r\n" + 
				"\r\n" + 
				"create index ix_periods_atrisk_start on #periods_atrisk (period_start_date);\r\n" + 
				"create index ix_periods_atrisk_end on #periods_atrisk (period_end_date);\r\n" + 
				"\r\n" + 
				"--{109 IN (@list_of_analysis_ids) | 110 IN (@list_of_analysis_ids) | 116 IN (@list_of_analysis_ids) | 117 IN (@list_of_analysis_ids)}?{\r\n" + 
				"\r\n" + 
				"IF OBJECT_ID('tempdb..#tmp_years', 'U') IS NOT NULL\r\n" + 
				"DROP TABLE  #tmp_years;\r\n" + 
				"\r\n" + 
				"IF OBJECT_ID('tempdb..#tmp_months', 'U') IS NOT NULL\r\n" + 
				"DROP TABLE  #tmp_months;\r\n" + 
				"\r\n" + 
				"WITH x AS (SELECT 0 AS n UNION ALL SELECT 1 AS n UNION ALL SELECT 2 AS n UNION ALL SELECT 3 AS n UNION ALL SELECT 4 AS n UNION ALL SELECT 5 AS n UNION ALL SELECT 6 AS n UNION ALL SELECT 7 AS n UNION ALL SELECT 8 AS n UNION ALL SELECT 9 AS n),\r\n" + 
				"    years AS (SELECT ones.n + 10*tens.n + 100*hundreds.n + 1000*thousands.n as year\r\n" + 
				"              FROM x ones,     x tens,      x hundreds,       x thousands)\r\n" + 
				"SELECT year\r\n" + 
				"INTO #tmp_years FROM years WHERE year BETWEEN 1900 AND 2201;\r\n" + 
				"\r\n" + 
				"WITH months AS (SELECT 1 AS month UNION ALL SELECT 2 AS month UNION ALL SELECT 3 AS month UNION ALL SELECT 4 AS month UNION ALL SELECT 5 AS month UNION ALL SELECT 6 AS month UNION ALL SELECT 7 AS month UNION ALL SELECT 8 AS month UNION ALL SELECT 9 AS month UNION ALL SELECT 10 AS month UNION ALL SELECT 11 AS month UNION ALL SELECT 12 AS month)\r\n" + 
				"SELECT month\r\n" + 
				"INTO #tmp_months FROM months;\r\n" + 
				"\r\n" + 
				"--}";
		//String sql = "IF OBJECT_ID('test', 'U') IS NULL CREATE TABLE test (	x BIGINT);";

		
		sql = "-- If results may be shown in comparison mode (there are several records for the same covariate ID in report),\r\n" + 
				"-- filter out only those covariates where all siblings are below threshold\r\n" + 
				"WITH threshold_passed_ids AS (\r\n" + 
				"  select covariate_id\r\n" + 
				"  from @results_database_schema.cc_results r\r\n" + 
				"  where r.cc_generation_id = @cohort_characterization_generation_id\r\n" + 
				"  GROUP BY r.type, r.fa_type, covariate_id\r\n" + 
				"  HAVING (r.fa_type <> 'PRESET' or r.type <> 'PREVALENCE' OR MAX(avg_value) > @threshold_level)\r\n" + 
				")\r\n" + 
				"select\r\n" + 
				"       r.type,\r\n" + 
				"       r.fa_type,\r\n" + 
				"       r.cc_generation_id,\r\n" + 
				"       r.analysis_id,\r\n" + 
				"       r.analysis_name,\r\n" + 
				"       r.covariate_id,\r\n" + 
				"       r.covariate_name,\r\n" + 
				"       c.concept_name,\r\n" + 
				"       r.time_window,\r\n" + 
				"       r.concept_id,\r\n" + 
				"       r.count_value,\r\n" + 
				"       r.avg_value,\r\n" + 
				"       r.stdev_value,\r\n" + 
				"       r.min_value,\r\n" + 
				"       r.p10_value,\r\n" + 
				"       r.p25_value,\r\n" + 
				"       r.median_value,\r\n" + 
				"       r.p75_value,\r\n" + 
				"       r.p90_value,\r\n" + 
				"       r.max_value,\r\n" + 
				"       r.cohort_definition_id,\r\n" + 
				"       r.strata_id,\r\n" + 
				"       r.strata_name\r\n" + 
				"from @results_database_schema.cc_results r\r\n" + 
				"  JOIN threshold_passed_ids tpi ON tpi.covariate_id = r.covariate_id\r\n" + 
				"  JOIN @vocabulary_schema.concept c on c.concept_id = r.concept_id\r\n" + 
				"where r.cc_generation_id = @cohort_characterization_generation_id\r\n" + 
				"";

		sql = "IF OBJECT_ID('@temp_database_schema.@target_table', 'U') IS NOT NULL DROP TABLE @temp_database_schema.@target_table;";
		String path = "inst/csv/replacementPatterns.csv";
		sql = SqlTranslate.translateSqlWithPath(sql, "spark", null, null, path);
		String connectionString = "jdbc:spark://";
		
		sql = BigQuerySparkTranslate.sparkHandleInsert(sql, connectionString);
		System.out.println(sql);
		
//		Pattern pattern = Pattern.compile("^((?!FROM).)*$");
//		System.out.println(pattern.matcher("SELECT * blaat b;").matches());
		
//		String sql = "SELECT * FROM table {@a = true} ?  {WHERE name = '@name'};";
//		sql = SqlRender.renderSql(sql, new String[]{"name", "a"}, new String[]{"NA\\joe", "true"});
//		System.out.println(sql);	
//		
//		String sourceSql = "SELECT TOP 10 * FROM my_table WHERE a = b;";
//		String sql;
//		sql = SqlTranslate.translateSqlWithPath(sourceSql, "postgresql", null, null, path);
//		System.out.println(sql);		
//		
//		sql = SqlTranslate.translateSqlWithPath(sourceSql, "oracle", null, null, path);
//		System.out.println(sql);
		
//		String sql = "SELECT * FROM @my_table";
//		for (String warning : SqlRender.checkSql(sql, new String[]{"my_table"}, new String[]{"asdfs"}))
//			System.out.println(warning);

//		String sql = "CREATE TABLE abcdefghijklmnopqrstuvwxyz1234567890;";
//		for (String warning : SqlTranslate.check(sql, ""))
//			System.out.println(warning);
	}
}
