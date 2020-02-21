package org.ohdsi.sql;

import java.sql.SQLException;

public class TestSqlRender {

	public static void main(String[] args) throws SQLException {
		
		String sql = "SELECT DISTINCT \n" +
				"  YEAR(observation_period_start_date)*100 + MONTH(observation_period_start_date) AS obs_month,\n" +
				"  CAST(CAST(YEAR(observation_period_start_date) AS VARCHAR(4)) + RIGHT('0' + CAST(MONTH(OBSERVATION_PERIOD_START_DATE) AS VARCHAR(2)), 2) + '01' AS DATE) AS obs_month_start,  \n" +
				"  DATEADD(dd,-1,DATEADD(mm,1,CAST(CAST(YEAR(observation_period_start_date) AS VARCHAR(4)) +  RIGHT('0' + CAST(MONTH(OBSERVATION_PERIOD_START_DATE) AS VARCHAR(2)), 2) + '01' AS DATE))) AS obs_month_end\n" +
				"INTO\n" +
				"  #temp_dates\n" +
				"FROM \n" +
				"  @cdm_database_schema.observation_period\n" +
				";";
		String path = "inst/csv/replacementPatterns.csv";
		sql = SqlTranslate.translateSqlWithPath(sql, "spark", null, null, path);

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