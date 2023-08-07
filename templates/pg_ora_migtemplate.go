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
	go_ora "github.com/sijms/go-ora/v2"
)

// Postgres DB Connection details
var (
	dbConPg  *pgx.Conn
	dbConOra *go_ora.Connection
	ctx         = context.Background()

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

	connora, err := go_ora.NewConnection("ORADBCONNSTR")
	if err != nil {
		log.Fatalf("Err conn: %v", err)
	}
	err = connora.Open()
	if err != nil {
		log.Fatalf("Err open: %v", err)
	}
	err = connora.Ping(ctx)
	if err != nil {
		log.Fatalf("Err conn: %v", err)
	}
	dbConOra = connora
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
	
	//////////////////////
	bulk := go_ora.NewBulkCopy(dbConOra, "TARGETTABLENAME")
	bulk.ColumnNames = []string{TARGETCOLUMNLIST}
	err = bulk.StartStream()
	if err != nil {
		log.Fatalln(err)
	}
	for _,rec := range TARGETSLICEDATA  {
		err = bulk.AddRow(TARGETSCAN)
		if err != nil {
			_ = bulk.Abort()
			log.Fatalln(err)
		}
	}
	err = bulk.EndStream()
	if err != nil {
		_ = bulk.Abort()
		log.Fatalln(err)
	}
	err = bulk.Commit()
	if err != nil {
		_ = bulk.Abort()
		log.Fatalln(err)
	}

	log.Println("process completed successfully.")
	defer dbConPg.Close(context.Background())
	defer dbConOra.Close()
}
