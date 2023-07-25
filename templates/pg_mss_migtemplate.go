package main

/////////////////////////////////////////////////////////////////////////
//      (c) 2023 Fujitsu Services, POA Account                         //
//       By: GyanPatel                                                 //
//       Ref:https://jira.apt.fs.fujitsu.com/jira/browse/EMIS-321      //
//       Date: 23-June-2023                                            //
//       Execution command:DXCTOTEMMIG.exe -env=/path/of/config/file   //
// Change History                                                      //
// Version    Author    Date           Desc                            //
// 1          Gyan      23-June-2023    Initial version                //
/////////////////////////////////////////////////////////////////////////

import (
	"context"
	"database/sql"
	"log"
	"strings"
	
	"github.com/jackc/pgx/v5"
	mssql "github.com/microsoft/go-mssqldb"
)

// Postgres DB Connection details
var (
	dbConPg  *pgx.Conn
	dbConMss *sql.DB
)


func init() {
	dbConnectString := "PGDBCONNSTR"
	config, err := pgx.ParseConfig(dbConnectString)
	if !strings.Contains(dbConnectString, "disable")  {
		config.TLSConfig.MinVersion = 1
	}

	if err != nil {
		log.Fatalln( "unable to parse:dbConnectString ", err)
	}
	conn, err := pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Fatalln( "unable to connect to PG database: ", err)
	}
	err = conn.Ping(context.Background())
	if err != nil {
		log.Fatalln( "unable to ping PG database:", err)
	}
	dbConPg = conn
	// below commented line for reference only
	//connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;", "test", "sa", "test", 1433, "test")
	connString := "MSSDBCONNSTR" //  trustservercertificate=true; encrypt=DISABLE; use this to diable ssl for DB connection

	connms, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatalln( "unable to connect to database:", err)
	}
	err = connms.Ping()
	if err != nil {
		log.Fatalln( "unable to ping to MS database:", err)
	}
	dbConMss = connms
}


COLLUMMETADATA_TYPESTRUCT

func main() {
	log.Println("startig the PG to SQL Server migation ")
	log.Println("querying Postgres table  ")
	rows, err := dbConPg.Query(context.Background(), "SOURCESELECTQUERY")
	if err != nil {
		log.Fatalln( "unexpected error when queriying table obc_interface:", err)
	}

	SOURCESLICEDEF
	for rows.Next() {
		SOURCEROWDEF
		err := rows.Scan(SORCESCANDEF)
		if err != nil {
			log.Fatalln( "unexpected error for rows.Values():", err)
		}
		SORCEROWAPPENDDEF
	}
	log.Println("querying table  completed ")
	if rows.Err() != nil {
		log.Fatalln( "Unexpected error for rows.Err():", rows.Err())
	}
	log.Println("begin sql server transaction ")
	// copy data to mssql table
	txn, err := dbConMss.Begin()
	if err != nil {
		log.Fatalln( "unable to being ms tranasaction ", err)
	}
	log.Println("prepare insert ")
	stmt, err := txn.Prepare(mssql.CopyIn("TARGETTABLENAME", mssql.BulkOptions{FireTriggers: false}, TARGETCOLUMNLIST))
	if err != nil {
		log.Fatalln( "unable to prepare stmt ", err)
	}
	log.Println("start data insert ")
	for _, rec := range TARGETSLICEDATA {
		_, err = stmt.Exec(TARGETSCAN)
		if err != nil {
			log.Fatalln( "unable to exec insert ", err)
		}
	}

	result, err := stmt.Exec()
	if err != nil {
		log.Fatalln( "unable to exec insert ", err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatalln( "unable to close insert stmt :", err)
	}
	log.Println("end data insert ")

	err = txn.Commit()
	if err != nil {
		log.Fatalln( "unable to commit transaction  :", err)
	}
	log.Println("insert  completed, transaction committed ")

	rowCount, _ := result.RowsAffected()
	log.Printf("rows copied into TABLE = <%d>\n", rowCount)
	log.Println("process completed successfully.")
	defer dbConPg.Close(context.Background())
	defer dbConMss.Close()
}
