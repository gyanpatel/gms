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
	dbConnectString := "dbname=postgres port=5432 user=postgres password=brdb host=10.185.246.85 sslmode=disable"
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
	connString := "server=10.185.244.91;user id=sa;password=Br1dgeV1ew;port=1433;database=mta;" //  trustservercertificate=true; encrypt=DISABLE; use this to diable ssl for DB connection

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


type OBC_INTERFACE struct {
branch_code  sql.NullString

ccn  sql.NullString

change_target_date  sql.NullTime

mta_mid_state  sql.NullString

mta_tid_state  sql.NullString

mta_update_tsmp  sql.NullTime

number_of_counters  sql.NullInt64

operation_code  sql.NullString

port_map  sql.NullString

seven_digit_branch_code  sql.NullString
}


func main() {
	log.Println("startig the PG to SQL Server migation ")
	log.Println("querying Postgres table  ")
	rows, err := dbConPg.Query(context.Background(), "select branch_code,ccn,change_target_date,mta_mid_state,mta_tid_state,mta_update_tsmp,number_of_counters,operation_code,port_map,seven_digit_branch_code from ops$brdb.OBC_INTERFACE")
	if err != nil {
		log.Fatalln( "unexpected error when queriying table obc_interface:", err)
	}

	var obc_interface []OBC_INTERFACE
	for rows.Next() {
		var recobc_interface OBC_INTERFACE
		err := rows.Scan(&recobc_interface.branch_code,&recobc_interface.ccn,&recobc_interface.change_target_date,&recobc_interface.mta_mid_state,&recobc_interface.mta_tid_state,&recobc_interface.mta_update_tsmp,&recobc_interface.number_of_counters,&recobc_interface.operation_code,&recobc_interface.port_map,&recobc_interface.seven_digit_branch_code,)
		if err != nil {
			log.Fatalln( "unexpected error for rows.Values():", err)
		}
		obc_interface = append( obc_interface, recobc_interface)
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
	stmt, err := txn.Prepare(mssql.CopyIn(".GYAN_EXTRACT_OBC_INTERFACE", mssql.BulkOptions{FireTriggers: false}, "branch_code","CCN","change_target_date","mta_mid_state","mta_tid_state","mta_update_tsmp","number_of_counters","operation_code","port_map","seven_digit_branch_code"))
	if err != nil {
		log.Fatalln( "unable to prepare stmt ", err)
	}
	log.Println("start data insert ")
	for _, rec := range obc_interface {
		_, err = stmt.Exec(rec.branch_code,rec.ccn,rec.change_target_date,rec.mta_mid_state,rec.mta_tid_state,rec.mta_update_tsmp,rec.number_of_counters,rec.operation_code,rec.port_map,rec.seven_digit_branch_code)
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
