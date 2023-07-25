package main

/////////////////////////////////////////////////////////////////////////
// Change History                                                      //
// Version    Author    Date           Desc                            //
// 1          Gyan      23-June-2023    Initial version                //
/////////////////////////////////////////////////////////////////////////

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	_ "github.com/microsoft/go-mssqldb"
)

// Postgres DB Connection details

type DbConnMaster struct {
	DbConPg              *pgx.Conn
	DbConMss             *sql.DB
	DbConOra             *sql.DB
	SrcConStr, TgtConStr string
}

var dbConnMaster DbConnMaster

type ConDeteail struct {
	Db     string `json:"db"`
	Host   string `json:"host"`
	DBUser string `json:"dbuser"`
	DBPort string `json:"dbport"`
	Pass   string `json:"pass"`
	DBName string `json:"dbname"`
	SSL    string `json:"sslmode"`
}

func init() {
	var srcConDet ConDeteail
	var tgtConDet ConDeteail
	srcDb, err := os.ReadFile("sourcedb.json")
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "unexpected to read config file ", err)
	}
	tgtDb, err := os.ReadFile("targetdb.json")
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "unexpected to read config file ", err)
	}
	err = json.Unmarshal(srcDb, &srcConDet)
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "unexpected to unmarshal config file ", err)
	}
	err = json.Unmarshal(tgtDb, &tgtConDet)
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "unexpected to unmarshal config file ", err)
	}
	dbConnMaster.DbConPg, dbConnMaster.SrcConStr, err = srcConDet.getPgDbConn()
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "error connecting to PG DB  ", err)
	}

	dbConnMaster.DbConMss, dbConnMaster.TgtConStr, err = tgtConDet.getMSSConn()
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "error connecting to PG DB  ", err)
	}
}

type TableMetaData struct {
	ColumnName string
	Datatype   string
}

var (
	tgtMetaData []TableMetaData
	srcMetaData []TableMetaData
	ctx         = context.Background()
)

func main() {

	srctab := flag.String("srctable", "", "name of the table ")
	tgtab := flag.String("tgttable", "", "name of the table ")
	scsh := flag.String("srsc", "", "name of the table ")
	tgsh := flag.String("tgsc", "", "name of the table ")
	srdb := flag.String("srdb", "", "name of the table ")
	tgdb := flag.String("tgdb", "", "name of the table ")

	flag.Parse()
	if len(*srctab) == 0 {
		log.Fatalln(GetCurrentFuncName(), `unexpected input flag, expected value e.g. -table="tablename"`)
	}
	if len(*tgtab) == 0 {
		log.Fatalln(GetCurrentFuncName(), `unexpected input flag, expected value e.g. -table="tablename"`)
	}
	migcode := *srdb + "_" + *tgdb
	srctable := strings.ToUpper(*srctab)
	tgtable := strings.ToUpper(*tgtab)
	lowersrctable := strings.ToLower(*srctab)
	meta := MetadataMaster{
		Srctab: srctable, Tgtab: tgtable, Srcsc: *scsh, Tgtsc: *tgsh, LowerSrctable: lowersrctable, MigCode: migcode}
	if strings.Compare(migcode, "pg_mss") == 0 {
		err := meta.PGtoMSSMig()
		if err != nil {
			log.Fatalln(GetCurrentFuncName(), "migration error ", migcode, err)
		}
	}
	if strings.Compare(migcode, "pg_ora") == 0 {
		err := meta.PGtoORAMig()
		if err != nil {
			log.Fatalln(GetCurrentFuncName(), "migration error ", migcode, err)
		}
	}
	log.Println(GetCurrentFuncName(), "process completed successfully.")
	defer dbConnMaster.DbConPg.Close(ctx)
	defer dbConnMaster.DbConMss.Close()
}
func (meta *MetadataMaster) PGtoMSSMig() (err error) {
	log.Println(GetCurrentFuncName(), "retrieving source metadata ")
	srcrows, err := dbConnMaster.DbConPg.Query(ctx, fmt.Sprintf(PGDefnSQL, meta.LowerSrctable))
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "unexpected error for Query:", err)
	}

	for srcrows.Next() {
		var metadata TableMetaData
		err := srcrows.Scan(&metadata.ColumnName, &metadata.Datatype)
		if err != nil {
			log.Fatalln(GetCurrentFuncName(), "unexpected error for rows.Values():", err)
		}

		srcMetaData = append(srcMetaData, metadata)
	}
	log.Println(GetCurrentFuncName(), "retrieving target metadata ", MSSDefnSQL, meta.Tgtab)
	tgtrows, err := dbConnMaster.DbConMss.Query(MSSDefnSQL, meta.Tgtab)
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "unexpected error for Query:", err)
	}

	for tgtrows.Next() {
		var metadata TableMetaData
		err := tgtrows.Scan(&metadata.ColumnName, &metadata.Datatype)
		if err != nil {
			log.Fatalln(GetCurrentFuncName(), "unexpected error for rows.Values():", err)
		}

		tgtMetaData = append(tgtMetaData, metadata)
	}
	meta.SrcMetaData = srcMetaData
	meta.TgtMetaData = tgtMetaData
	err = meta.CreateGoFile()
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), GetCurrentFuncName(), "unexpected error calling CreateGoFile", err)
	}
	return
}
func (meta *MetadataMaster) CreateGoFile() (err error) {
	crType, selectQuery, srcScanDef, tgtColList, tgtScanList, err := meta.CreateType()
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "unexpected error calling CreateType", err)
	}
	template, err := os.ReadFile("templates/" + meta.MigCode + "_migtemplate.go")
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "error reading the template ", err)
	}
	migFileCont := strings.Replace(string(template), "COLLUMMETADATA_TYPESTRUCT", crType, -1)
	migFileCont = strings.Replace(migFileCont, "PGDBCONNSTR", dbConnMaster.SrcConStr, -1)
	migFileCont = strings.Replace(migFileCont, "MSSDBCONNSTR", dbConnMaster.TgtConStr, -1)
	sourceSelectQuery := fmt.Sprintf("select %s from %s.%s", selectQuery, meta.Srcsc, meta.Srctab)
	migFileCont = strings.Replace(migFileCont, "SOURCESELECTQUERY", sourceSelectQuery, -1)
	migFileCont = strings.Replace(migFileCont, "SOURCESLICEDEF", fmt.Sprintf("var %s []%s", meta.LowerSrctable, meta.Srctab), -1)
	migFileCont = strings.Replace(migFileCont, "SOURCEROWDEF", fmt.Sprintf("var rec%s %s", meta.LowerSrctable, meta.Srctab), -1)
	migFileCont = strings.Replace(migFileCont, "SORCEROWAPPENDDEF", fmt.Sprintf("%s = append( %s, rec%s)", meta.LowerSrctable, meta.LowerSrctable, meta.LowerSrctable), -1)
	migFileCont = strings.Replace(migFileCont, "SORCESCANDEF", srcScanDef, -1)
	migFileCont = strings.Replace(migFileCont, "TARGETTABLENAME", meta.Tgtab, -1)
	migFileCont = strings.Replace(migFileCont, "TARGETCOLUMNLIST", tgtColList, -1)
	migFileCont = strings.Replace(migFileCont, "TARGETSLICEDATA", meta.LowerSrctable, -1)
	migFileCont = strings.Replace(migFileCont, "TARGETSCAN", tgtScanList, -1)
	dir := "migration/pgtomssmig/" + meta.Srctab
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, 0644)
		if err != nil {
			log.Fatalln(GetCurrentFuncName(), "error creating dir ", err)
		}
	}
	err = os.WriteFile(filepath.Clean(dir+"/"+meta.Srctab+".go"), []byte(migFileCont), 0644)
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "error creating migration go file ", err)
	}
	err = runGo(dir, meta.Srctab)
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "error compiling migration go file ", err)
	}
	return
}

type MetadataMaster struct {
	Srctab, Tgtab, Srcsc, Tgtsc, LowerSrctable, MigCode, SrcConStr, TgtConStr string
	SrcMetaData, TgtMetaData                                                  []TableMetaData
}

func (meta *MetadataMaster) CreateType() (structType, selectQuery, srcScanDef, tgtColList, tgtScanList string, err error) {
	structType = "type " + strings.ToUpper(meta.Srctab) + " struct {"
	for _, rec := range meta.TgtMetaData {
		tgtColList = tgtColList + fmt.Sprintf(`"%s",`, rec.ColumnName)

	}
	for _, rec := range meta.SrcMetaData {
		var dType string
		selectQuery = selectQuery + rec.ColumnName + ","
		srcScanDef = srcScanDef + fmt.Sprintf("&rec%s.%s,", meta.LowerSrctable, rec.ColumnName)
		tgtScanList = tgtScanList + fmt.Sprintf("rec.%s,", rec.ColumnName)

		if strings.Contains(strings.ToLower(rec.Datatype), "var") {
			dType = "sql.NullString"
		} else if strings.Contains(strings.ToLower(rec.Datatype), "int") {
			dType = "sql.NullInt64"
		} else if strings.Contains(strings.ToLower(rec.Datatype), "date") {
			dType = "sql.NullTime"
		} else if strings.Contains(strings.ToLower(rec.Datatype), "time") {
			dType = "sql.NullTime"
		} else if strings.Contains(strings.ToLower(rec.Datatype), "num") {
			dType = " sql.NullFloat64"
		} else {
			dType = "sql.NullString"
		}
		recType := rec.ColumnName + "  " + dType
		structType = structType + "\n" + recType + "\n"
	}
	tgtColList = tgtColList[:len(tgtColList)-1]
	selectQuery = selectQuery[:len(selectQuery)-1]
	tgtScanList = tgtScanList[:len(tgtScanList)-1]

	structType = structType + "}\n"
	return
}

func runGo(dir, mod string) (err error) {
	fileName := dir + "/" + mod + ".go"
	log.Println(GetCurrentFuncName(), "running...", fileName)
	cmd := exec.Command("go", "run", fileName)
	if err = cmd.Run(); err != nil {
		log.Println(GetCurrentFuncName(), "error running cmd", cmd.Stdout, cmd.Stderr, err)
		return
	}
	log.Println(GetCurrentFuncName(), "outout ", cmd.Stdout)
	return
}
func GetCurrentFuncName() string {
	pc, _, line, _ := runtime.Caller(1)
	return runtime.FuncForPC(pc).Name() + " line-" + strconv.Itoa(line)
}

func (meta *MetadataMaster) PGtoORAMig() (err error) {
	log.Println(GetCurrentFuncName(), "retrieving source metadata ")
	srcrows, err := dbConnMaster.DbConPg.Query(context.Background(), fmt.Sprintf(PGDefnSQL, meta.LowerSrctable))
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "unexpected error for Query:", err)
	}

	for srcrows.Next() {
		var metadata TableMetaData
		err := srcrows.Scan(&metadata.ColumnName, &metadata.Datatype)
		if err != nil {
			log.Fatalln(GetCurrentFuncName(), "unexpected error for rows.Values():", err)
		}

		srcMetaData = append(srcMetaData, metadata)
	}
	log.Println(GetCurrentFuncName(), "retrieving target metadata ")
	tgtrows, err := dbConnMaster.DbConMss.Query(fmt.Sprintf(ORADefnSQL, meta.Tgtab))
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), "unexpected error for Query:", err)
	}

	for tgtrows.Next() {
		var metadata TableMetaData
		err := tgtrows.Scan(&metadata.ColumnName, &metadata.Datatype)
		if err != nil {
			log.Fatalln(GetCurrentFuncName(), "unexpected error for rows.Values():", err)
		}

		tgtMetaData = append(tgtMetaData, metadata)
	}
	log.Println(GetCurrentFuncName(), srcMetaData, tgtMetaData)
	meta.SrcMetaData = srcMetaData
	meta.TgtMetaData = tgtMetaData
	log.Println("meta value ", meta)
	err = meta.CreateGoFile()
	if err != nil {
		log.Fatalln(GetCurrentFuncName(), GetCurrentFuncName(), "unexpected error calling CreateGoFile", err)
	}
	return
}

func (conDeteail *ConDeteail) getPgDbConn() (conn *pgx.Conn, dbConstrPg string, err error) {
	dbConstrPg = "dbname=" + conDeteail.DBName + " port=" + conDeteail.DBPort + " user=" + conDeteail.DBUser + " password=" + conDeteail.Pass + " host=" + conDeteail.Host + " sslmode=" + conDeteail.SSL
	config, err := pgx.ParseConfig(dbConstrPg)
	if strings.Compare(conDeteail.SSL, "disable") != 0 {
		config.TLSConfig.MinVersion = 1
	}

	if err != nil {
		log.Println(GetCurrentFuncName(), "Unable to parse: ", err)
	}
	conn, err = pgx.ConnectConfig(context.Background(), config)
	if err != nil {
		log.Println(GetCurrentFuncName(), "Unable to connect to PG database: ", err)
	}
	err = conn.Ping(context.Background())
	if err != nil {
		log.Println(GetCurrentFuncName(), "Unable to ping PG database:", err)
	}
	return
}

func (conDeteail *ConDeteail) getMSSConn() (conn *sql.DB, dbConstrMss string, err error) {
	dbConstrMss = fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;", conDeteail.Host, conDeteail.DBUser, conDeteail.Pass, conDeteail.DBPort, conDeteail.DBName)
	conn, err = sql.Open(conDeteail.Db, dbConstrMss)
	if err != nil {
		log.Println(GetCurrentFuncName(), "Unable to connect to database:", err)
	}
	err = conn.Ping()
	if err != nil {
		log.Println(GetCurrentFuncName(), "Unable to connect to MS database:", err)
	}
	return
}
