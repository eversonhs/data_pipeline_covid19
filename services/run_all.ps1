$rw_tr_scripts='rw_tr_vacinacao_covid_19.py'
$tr_rf_dm_scripts='tr_rf_dm_campanhas.py', 'tr_rf_dm_estabelecimentos.py', 'tr_rf_dm_vacinas.py', 'tr_rf_dm_municipios.py', 'tr_rf_dm_pacientes.py'
$tr_rf_ft_scripts='tr_rf_ft_vacinacao.py'
$scripts=$rw_tr_scripts, $tr_rf_dm_scripts, $tr_rf_ft_scripts

foreach ($script in $scripts)  {
    docker exec spark-client bash -c 'spark-submit --master $SPARK_MASTER_URI --jars $MYSQL_CONNECTORJ_PATH --driver-class-path $MYSQL_CONNECTORJ_PATH --executor-memory 1g --num-executors 2 --executor-cores 1 ./jobs/${1}' -f $script

}

