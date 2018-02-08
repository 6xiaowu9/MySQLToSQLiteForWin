<?php 

/**
* 	数据迁移 mysql to sqlite
* 	需要根据自我需求修改配置。
* 	本地迁移 使用 exportTableToCsv_v1 方法性能好一些
* 	远程迁移 使用 exportTableToCsv 方法
*/
class ConvertDB
{
	// 数据库配置
	private $host = 'localhost';
	private $dbname = 'test';
	private $username ='root';
	private $password = '12345678';
	private $mysql;

	// 根据实际情况配置
	private $memory = "512M";

	// 需要迁移的mysql数据表 
	private $tables = [
		'test',
	];

	// 是否加密
	private $private = false;

	// 数据库对应密码
	private $key = [
		'test' => 'TEST_KEY',
	];

	// 数据库迁移地址
	private $DBToFile = '.\db';
	function __construct( )
	{
		$this->mysql = $this->mysql();
		$this->createFile("cache");
		$this->createFile("db");
		ini_set("memory_limit", $this->memory );
		$this->start( $this->tables );
	}

	function start( $tables ){
		if( count( $tables ) && !is_array( $tables ) )
			die("缺少表名");
		$commands = "";
		foreach ( $tables as $table ) {
			if( !$table ){ echo ('表名错误');break;}
			$export_csv = $this->exportTableToCsv_v1( $table , $this->convertTable( $table ));
			if( $this->private ){
				$commands .= ".\sqlite\sqlite3.exe .\cache\\{$table}.db < .\cache\\{$table}.txt\r\necho .\r\ndel /Q {$this->DBToFile}\\{$table}.db\r\necho .\r\n.\sqlite\sqlcipher.exe .\cache\\{$table}.db < .\cache\\{$table}_key.txt\r\n";
			}else{
				$commands .= "del /Q {$this->DBToFile}\\{$table}.db\r\necho .\r\n.\sqlite\sqlite3.exe {$this->DBToFile}\\{$table}.db < .\cache\\{$table}.txt\r\n";
			}
		}
		$this->genarateConvertCommandsBAT("@echo off\r\n".$commands."echo ---------------------完成迁移-------------------\r\necho MySQL to SQLite. File:{$this->DBToFile}\r\n");
		echo "生成完成！\r\n";
		echo "开始迁移...\r\n";
		system( ".\cache\convert.bat", $array );

	}

	public function convertTable( $table ){
		$this->sqlite = $this->sqlite( $table );
		return $this->createSqliteTable($table);
	}

	public function createSqliteTable( $table ){
		$res = $this->mysql->query("desc {$table}");
		$create = "DROP TABLE IF EXISTS \"{$table}\";CREATE TABLE \"{$table}\"(";
		$columns = "";
		foreach ($res as $row) {
			$create .= " \"{$row['Field']}\" ".( strpos(strtolower($row['Type']), 'int') !== false ? 'INTEGER' : 'TEXT' ).( $row['Key'] == 'PRI' ? ' NOT NULL PRIMARY KEY '.( $row['Extra'] == 'auto_increment' ? 'AUTOINCREMENT' : '' ) : '' ).',';
			$columns .= " ifnull(`{$row['Field']}`, '') `{$row['Field']}`, ";
		}
		$create = substr($create, 0 , -1).");";
		$this->sqlite->exec( $create );
		$this->createSqliteIndex( $table );
		return substr($columns, 0 , -2);
	}

	public function createSqliteIndex( $table ){
		$res = $this->mysql->query("show index from {$table}");
		$index = "";
		foreach ($res as $row) {
			if( $row['Key_name'] != 'PRIMARY')
				$index .= " CREATE INDEX \"{$row['Key_name']}\" ON \"{$table}\" ( \"{$row['Column_name']}\" ASC ); ";
		}
		$this->sqlite->exec( $index."PRAGMA foreign_keys = true;" );
	}


	public function exportTableToCsv( $table , $columns){
		$export_csv = str_replace('\\', '/', __DIR__)."/../cache/{$table}.csv";
		if( !( !file_exists("./cache/{$table}.csv") || unlink("./cache/{$table}.csv") ) )
			echo "----删除失败：./cache/{$table}.csv \r\n";
		$this->fp = fopen( $export_csv, "a");
		$res = $this->mysql->query("select {$columns} from {$table}");
		$page_size = 10000;
		$res = array_chunk( $res->fetchAll(PDO::FETCH_ASSOC), $page_size);
		foreach ( $res as $data ) {
			$csv = "";
			foreach ($data as $row) {
				$str = "";
				foreach ($row as $value) {
					$str .= "\"".str_replace("\"","\"\"", $value)."\",";
				}
				$csv .= substr( $str, 0, -1 )."\r\n";
			}
			$this->genarateCsv( $csv );
		}
		fclose($this->fp);
		echo "----导出完成：./cache/{$table}.csv \r\n";
		if( !$this->genarateCommands( $table, $export_csv ) )
			die("ERROR {$table}");
		return $export_csv;
	}

	public function exportTableToCsv_v1( $table , $columns){
		$export_csv = str_replace('\\', '/', __DIR__)."/../cache/{$table}.csv";
		$sql = "select {$columns} from {$table}
				into outfile '{$export_csv}'   
				fields terminated by ',' optionally enclosed by '\"' escaped by '\"'   
				lines terminated by '\r\n';" ;
		$exec = $this->mysql->exec($sql);
		if( !$this->genarateCommands( $table, $export_csv ) )
			die("ERROR {$table}");
		return $export_csv;
	}

	public function genarateCommands( $table, $export_csv ){
		$this->writeCommandsTXT( $table, $this->genarateCommandsTXT( $table, $export_csv ) );
		return $this->writeCommandsTXT( $table."_key", $this->genarateKeyCommandsTXT( $table, $this->DBToFile ) );
	}

	public function sqlite( $table ){
    	return new PDO( "sqlite:".__DIR__."/../cache/{$table}.db", '', '');
	}

	public function mysql(){
		$options = array(
		    PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES utf8',
		);
		return new PDO("mysql:host={$this->host};dbname={$this->dbname}", $this->username, $this->password, $options);  
	}

	public function writeCommandsTXT( $table, $commands ){
		$fp = fopen("./cache/{$table}.txt", "w+");
		if($fp){ 
			$flag=fwrite($fp, $commands); 
			if(!$flag) { 
				echo "写入文件失败\n"; 
			}
		}else{ 
			echo "打开文件失败\n"; 
		}
		fclose($fp);
		return true;
	}

	public function genarateCommandsTXT( $table, $export_csv ){
		return ".mode csv\r\n.import {$export_csv} {$table}";
	}
	public function genarateKeyCommandsTXT( $table, $DBToFile ){
		return "ATTACH DATABASE '{$DBToFile}\\{$table}.db' AS encrypted KEY '{$this->key[$table]}';\r\nSELECT sqlcipher_export('encrypted');\r\nDETACH DATABASE encrypted;";
	}


	public function genarateConvertCommandsBAT( $commands ){
		$fp = fopen("./cache/convert.bat", "w+");
		if($fp){ 
			$flag=fwrite($fp, $commands); 
			if(!$flag) { 
				echo "写入文件失败\n"; 
			}
		}else{ 
			echo "打开文件失败\n"; 
		}
		fclose($fp);
		return true;
	}
	public function genarateCsv( $csv ){
		if( $this->fp ){ 
			$flag=fwrite($this->fp, $csv); 
			if(!$flag) { 
				echo "写入文件失败\n"; 
			}
		}else{ 
			echo "打开文件失败\n"; 
		}
		return true;
	}

	public function createFile( $dir ){
		if (!file_exists($dir)){
            mkdir ($dir,0777,true);
        }
	}
}
new ConvertDB();
