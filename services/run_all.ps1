$scripts='rw_tr_vacinacao_covid_19.py', 'tr_rf_dm_campanhas.py', 'tr_rf_dm_estabelecimentos.py', 'tr_rf_dm_vacinas.py', 'tr_rf_dm_municipios.py', 'tr_rf_dm_pacientes.py', 'tr_rf_ft_vacinacao.py'

foreach ($script in $scripts)  {
    docker exec spark-client bash -c 'spark-submit --master $SPARK_MASTER_URI --jars $POSTGRESQL_JDBC_DRIVER --driver-class-path $POSTGRESQL_JDBC_DRIVER --executor-memory 8g --executor-cores 4 ./jobs/bulk/${1}' -f $script

}

