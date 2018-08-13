CREATE schema `testeMac`
use `testeMac`

CREATE TABLE teste (
  id int not null AUTO_INCREMENT,
  timesta varchar(20) not null,
  rssi varchar(5) NOT NULL,
  peermac varchar(30) NOT NULL,
  PRIMARY KEY (id)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/Limpos_sem_Mac_Falso.csv'
INTO TABLE teste
CHARACTER SET utf8
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r'
#IGNORE 1 ROWS #Comando para ignorar a primeira linha do arquivo .txt
(timesta,rssi,peermac);

#select id from dados where data1 like "2018-06-12%";