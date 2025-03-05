package canal

import (
	"context"
	"sync/atomic"
	"time"
	"strings"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/go-mysql-org/go-mysql/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

func (c *Canal) startSyncer() (*replication.BinlogStreamer, error) {
	gset := c.master.GTIDSet()
	if gset == nil || gset.String() == "" {
		pos := c.master.Position()
		s, err := c.syncer.StartSync(pos)
		if err != nil {
			return nil, errors.Errorf("start sync replication at binlog %v error %v", pos, err)
		}
		c.cfg.Logger.Infof("start sync binlog at binlog file %v", pos)
		return s, nil
	} else {
		gsetClone := gset.Clone()
		s, err := c.syncer.StartSyncGTID(gset)
		if err != nil {
			return nil, errors.Errorf("start sync replication at GTID set %v error %v", gset, err)
		}
		c.cfg.Logger.Infof("start sync binlog at GTID set %v", gsetClone)
		return s, nil
	}
}

func (c *Canal) runSyncBinlog() error {
    defer func() {
        c.cancel()
    }()

    for {
        select {
        case <-c.ctx.Done():
            return nil
        default:
            pos := c.master.Position()
            c.cfg.Logger.Infof("begin to re-sync from (%s, %d)", pos.Name, pos.Pos)
            streamer, err := c.syncer.StartSync(pos)
            if err != nil {
                c.cfg.Logger.Errorf("start sync binlog failed: %v", err)
                if strings.Contains(err.Error(), "ERROR 1236 (HY000)") && strings.Contains(err.Error(), "position > file size") {
                    c.cfg.Logger.Warnf("position %d exceeds file size in %s, attempting recovery", pos.Pos, pos.Name)
                    if newPos, recoveryErr := c.recoverFromPositionError(pos); recoveryErr == nil {
                        c.cfg.Logger.Infof("recovered to new position: %s, %d", newPos.Name, newPos.Pos)
                        c.master.Update(newPos)
                        continue
                    } else {
                        c.cfg.Logger.Errorf("recovery failed: %v", recoveryErr)
                        return errors.Trace(recoveryErr)
                    }
                }
                return errors.Trace(err)
            }

            for {
                ev, err := streamer.GetEvent(c.ctx)
                if err != nil {
                    if strings.Contains(err.Error(), "ERROR 1236 (HY000)") && strings.Contains(err.Error(), "position > file size") {
                        c.cfg.Logger.Warnf("position error detected during sync at %s:%d, attempting recovery", pos.Name, pos.Pos)
                        if newPos, recoveryErr := c.recoverFromPositionError(pos); recoveryErr == nil {
                            c.cfg.Logger.Infof("recovered to new position: %s, %d", newPos.Name, newPos.Pos)
                            c.master.Update(newPos)
                            break
                        } else {
                            c.cfg.Logger.Errorf("recovery failed: %v", recoveryErr)
                            return errors.Trace(recoveryErr)
                        }
                    }
                    if errors.Cause(err) == context.Canceled {
                        return nil
                    }
                    c.cfg.Logger.Errorf("get binlog event failed: %v", err)
                    return errors.Trace(err)
                }

                c.updateReplicationDelay(ev)

                switch e := ev.Event.(type) {
                case *replication.RotateEvent:
                    if ev.Header.Timestamp == 0 {
                        fakeRotateLogName := string(e.NextLogName)
                        c.cfg.Logger.Infof("received fake rotate event, next log name is %s", fakeRotateLogName)
                        if fakeRotateLogName != c.master.Position().Name {
                            c.cfg.Logger.Info("log name changed, treating fake rotate as real")
                        } else {
                            continue
                        }
                    }
                }

                if err := c.handleEvent(ev); err != nil {
                    return errors.Trace(err)
                }
            }
        }
    }
}

// Placeholder for required functions (normally in canal.go)
func (c *Canal) recoverFromPositionError(pos mysql.Position) (mysql.Position, error) {
    res, err := c.Execute("SHOW BINARY LOGS")
    if err != nil {
        return pos, errors.Annotate(err, "failed to query binary logs for recovery")
    }

    currentFile := pos.Name
    var nextFile string
    foundCurrent := false

    for i := 0; i < res.RowNumber(); i++ {
        logFile, _ := res.GetString(i, 0)
        if foundCurrent && nextFile == "" {
            nextFile = logFile
        }
        if logFile == currentFile {
            foundCurrent = true
        }
    }

    if nextFile == "" {
        return pos, errors.New("no next binlog file available for recovery")
    }

    newPos := mysql.Position{Name: nextFile, Pos: 4}
    return newPos, nil
}

func (c *Canal) handleEvent(ev *replication.BinlogEvent) error {
	savePos := false
	force := false
	pos := c.master.Position()
	var err error

	curPos := pos.Pos

	// next binlog pos
	pos.Pos = ev.Header.LogPos

	// We only save position with RotateEvent and XIDEvent.
	// For RowsEvent, we can't save the position until meeting XIDEvent
	// which tells the whole transaction is over.
	// TODO: If we meet any DDL query, we must save too.
	switch e := ev.Event.(type) {
	case *replication.RotateEvent:
		pos.Name = string(e.NextLogName)
		pos.Pos = uint32(e.Position)
		c.cfg.Logger.Infof("rotate binlog to %s", pos)
		savePos = true
		force = true
		if err = c.eventHandler.OnRotate(ev.Header, e); err != nil {
			return errors.Trace(err)
		}
	case *replication.RowsEvent:
		// we only focus row based event
		err = c.handleRowsEvent(ev)
		if err != nil {
			c.cfg.Logger.Errorf("handle rows event at (%s, %d) error %v", pos.Name, curPos, err)
			return errors.Trace(err)
		}
		return nil
	case *replication.TransactionPayloadEvent:
		// handle subevent row by row
		ev := ev.Event.(*replication.TransactionPayloadEvent)
		for _, subEvent := range ev.Events {
			err = c.handleEvent(subEvent)
			if err != nil {
				c.cfg.Logger.Errorf("handle transaction payload subevent at (%s, %d) error %v", pos.Name, curPos, err)
				return errors.Trace(err)
			}
		}
		return nil
	case *replication.XIDEvent:
		savePos = true
		// try to save the position later
		if err := c.eventHandler.OnXID(ev.Header, pos); err != nil {
			return errors.Trace(err)
		}
		if e.GSet != nil {
			c.master.UpdateGTIDSet(e.GSet)
		}
	case *replication.MariadbGTIDEvent:
		if err := c.eventHandler.OnGTID(ev.Header, e); err != nil {
			return errors.Trace(err)
		}
	case *replication.GTIDEvent:
		if err := c.eventHandler.OnGTID(ev.Header, e); err != nil {
			return errors.Trace(err)
		}
	case *replication.RowsQueryEvent:
		if err := c.eventHandler.OnRowsQueryEvent(e); err != nil {
			return errors.Trace(err)
		}
	case *replication.QueryEvent:
		stmts, _, err := c.parser.Parse(string(e.Query), "", "")
		if err != nil {
			// The parser does not understand all syntax.
			// For example, it won't parse [CREATE|DROP] TRIGGER statements.
			c.cfg.Logger.Errorf("parse query(%s) err %v, will skip this event", e.Query, err)
			return nil
		}
		if len(stmts) > 0 {
			savePos = true
		}
		for _, stmt := range stmts {
			nodes := parseStmt(stmt)
			for _, node := range nodes {
				if node.db == "" {
					node.db = string(e.Schema)
				}
				if err = c.updateTable(ev.Header, node.db, node.table); err != nil {
					return errors.Trace(err)
				}
			}
			if len(nodes) > 0 {
				force = true
				// Now we only handle Table Changed DDL, maybe we will support more later.
				if err = c.eventHandler.OnDDL(ev.Header, pos, e); err != nil {
					return errors.Trace(err)
				}
			}
		}
		if savePos && e.GSet != nil {
			c.master.UpdateGTIDSet(e.GSet)
		}
	default:
		return nil
	}

	if savePos {
		c.master.Update(pos)
		c.master.UpdateTimestamp(ev.Header.Timestamp)

		if err := c.eventHandler.OnPosSynced(ev.Header, pos, c.master.GTIDSet(), force); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

type node struct {
	db    string
	table string
}

func parseStmt(stmt ast.StmtNode) (ns []*node) {
	switch t := stmt.(type) {
	case *ast.RenameTableStmt:
		ns = make([]*node, len(t.TableToTables))
		for i, tableInfo := range t.TableToTables {
			ns[i] = &node{
				db:    tableInfo.OldTable.Schema.String(),
				table: tableInfo.OldTable.Name.String(),
			}
		}
	case *ast.AlterTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	case *ast.DropTableStmt:
		ns = make([]*node, len(t.Tables))
		for i, table := range t.Tables {
			ns[i] = &node{
				db:    table.Schema.String(),
				table: table.Name.String(),
			}
		}
	case *ast.CreateTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	case *ast.TruncateTableStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	case *ast.CreateIndexStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	case *ast.DropIndexStmt:
		n := &node{
			db:    t.Table.Schema.String(),
			table: t.Table.Name.String(),
		}
		ns = []*node{n}
	}
	return ns
}

func (c *Canal) updateTable(header *replication.EventHeader, db, table string) (err error) {
	c.ClearTableCache([]byte(db), []byte(table))
	c.cfg.Logger.Infof("table structure changed, clear table cache: %s.%s\n", db, table)
	if err = c.eventHandler.OnTableChanged(header, db, table); err != nil && errors.Cause(err) != schema.ErrTableNotExist {
		return errors.Trace(err)
	}
	return
}

func (c *Canal) updateReplicationDelay(ev *replication.BinlogEvent) {
	var newDelay uint32
	now := uint32(utils.Now().Unix())
	if now >= ev.Header.Timestamp {
		newDelay = now - ev.Header.Timestamp
	}
	atomic.StoreUint32(c.delay, newDelay)
}

func (c *Canal) handleRowsEvent(e *replication.BinlogEvent) error {
	ev := e.Event.(*replication.RowsEvent)

	// Caveat: table may be altered at runtime.
	schemaName := string(ev.Table.Schema)
	tableName := string(ev.Table.Table)

	t, err := c.GetTable(schemaName, tableName)
	if err != nil {
		e := errors.Cause(err)
		// ignore errors below
		if e == ErrExcludedTable || e == schema.ErrTableNotExist || e == schema.ErrMissingTableMeta {
			err = nil
		}

		return err
	}
	var action string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
		action = InsertAction
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		action = DeleteAction
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2, replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
		action = UpdateAction
	default:
		return errors.Errorf("%s not supported now", e.Header.EventType)
	}
	events := newRowsEvent(t, action, ev.Rows, e.Header)
	return c.eventHandler.OnRow(events)
}

func (c *Canal) FlushBinlog() error {
	_, err := c.Execute("FLUSH BINARY LOGS")
	return errors.Trace(err)
}

func (c *Canal) WaitUntilPos(pos mysql.Position, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	for {
		select {
		case <-timer.C:
			return errors.Errorf("wait position %v too long > %s", pos, timeout)
		default:
			if !c.cfg.DisableFlushBinlogWhileWaiting {
				err := c.FlushBinlog()
				if err != nil {
					return errors.Trace(err)
				}
			}
			curPos := c.master.Position()
			if curPos.Compare(pos) >= 0 {
				return nil
			} else {
				c.cfg.Logger.Debugf("master pos is %v, wait catching %v", curPos, pos)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func (c *Canal) GetMasterPos() (mysql.Position, error) {
	showBinlogStatus := "SHOW BINARY LOG STATUS"
	if eq, err := c.conn.CompareServerVersion("8.4.0"); (err == nil) && (eq < 0) {
		showBinlogStatus = "SHOW MASTER STATUS"
	}

	rr, err := c.Execute(showBinlogStatus)
	if err != nil {
		return mysql.Position{}, errors.Trace(err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return mysql.Position{Name: name, Pos: uint32(pos)}, nil
}

func (c *Canal) GetMasterGTIDSet() (mysql.GTIDSet, error) {
	query := ""
	switch c.cfg.Flavor {
	case mysql.MariaDBFlavor:
		query = "SELECT @@GLOBAL.gtid_current_pos"
	default:
		query = "SELECT @@GLOBAL.GTID_EXECUTED"
	}
	rr, err := c.Execute(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gx, err := rr.GetString(0, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	gset, err := mysql.ParseGTIDSet(c.cfg.Flavor, gx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return gset, nil
}

func (c *Canal) CatchMasterPos(timeout time.Duration) error {
	pos, err := c.GetMasterPos()
	if err != nil {
		return errors.Trace(err)
	}

	return c.WaitUntilPos(pos, timeout)
}
