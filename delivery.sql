CREATE TABLE `delivery` (
  `xid` bigint(20) NOT NULL AUTO_INCREMENT,
  `qid` char(12) DEFAULT NULL,
  `tstamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `sender` char(255) DEFAULT NULL,
  `recipient` char(255) DEFAULT NULL,
  `status` char(15) DEFAULT NULL,
  `msg` varchar(1024) DEFAULT NULL,
  PRIMARY KEY (`xid`),
  KEY `xtstamp` (`tstamp`),
  KEY `xsender` (`sender`(191)),
  KEY `xrecipient` (`recipient`(191))
) ENGINE=InnoDB AUTO_INCREMENT=2631 DEFAULT CHARSET=utf8mb4;

