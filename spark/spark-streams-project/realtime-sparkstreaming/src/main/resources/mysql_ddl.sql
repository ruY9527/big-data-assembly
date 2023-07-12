CREATE TABLE `gmvstats` (
                            `stat_date` DATE NOT NULL,
                            `stat_hour` VARCHAR(10) NOT NULL,
                            `gmv` DECIMAL(20,2) DEFAULT NULL,
                            PRIMARY KEY (`stat_date`,`stat_hour`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

