import os.path
import yaml
import logging
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import RotateEvent, XidEvent

class Main:
    log_pos, log_file = None, None

    def __init__(self):
        try:
            self.config = yaml.load(open(sys.argv[1]))
        except IndexError:
            print('Error: not specify config file')
            exit(1)

        mysql = self.config.get('mysql')

        self.tables = mysql.get('tables')
        mysql.update({
            'tables': ' '.join(mysql.get('tables'))
        })
        self.master = self.tables[0]
        self.current_table = None

        self.connection = dict(
            [(key, self.config['mysql'][key]) for key in ['host', 'port', 'user', 'password', 'database']]
        )

        binlog_path = self.config['binlog']['file']
        if os.path.isfile(binlog_path):
            with open(binlog_path, 'r') as f:
                record = yaml.load(f)
                self.log_file = record.get('log_file')
                self.log_pos = record.get('log_pos')

    def _is_sync(self):
        result = bool(self.log_file and self.log_pos)
        return result

    def _save_binlog_record(self):
        if self._is_sync:
            with open(self.config['binlog']['file'], 'w') as f:
                logging.info("Sync binlog_file: {file}  binlog_pos: {pos}".format(file=self.log_file, pos=self.log_pos))
                yaml.safe_dump({
                    "log_file": self.log_file,
                    "log_pos": self.log_pos
                    }, f, default_flow_style=False)

    def _init_logging(self):
        logging.basicConfig(filename=self.config['logging']['file'],
                            level=logging.INFO,
                            format='[%(levelname)s] - %(filename)s[line:%(lineno)d] - %(asctime)s %(message)s')
        self.logger = logging.getLogger(__name__)

        def cleanup(*args):
            self.logger.info('Received stop signal')
            self.logger.info('Shutdown')
            sys.exit(0)

        signal.signal(signal.SIGINT, cleanup)
        signal.signal(signal.SIGTERM, cleanup)

    def _binlog_loader(self):
        if self.is_binlog_sync:
            resume_stream = True
            logging.info("Resume from binlog_file: {file} & binlog_pos: {pos}".format(file=self.log_file, pos=self.log_pos))
        else:
            resume_stream = False

        stream = BinLogStreamReader(connection_settings=self.connection,
                                    server_id=self.config['mysql']['server_id'],
                                    only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent, RotateEvent, XidEvent],
                                    only_tables=self.tables,
                                    resume_stream=resume_stream,
                                    blocking=True,
                                    log_file=self.log_file,
                                    log_pos=self.log_pos)
        for binlogevent in stream:
            self.log_file = stream.log_file
            self.log_pos = stream.log_pos

            # RotateEvent to update binlog record when no related table changed
            if isinstance(binlogevent, RotateEvent):
                self._save_binlog_record()
                continue

            if isinstance(binlogevent, XidEvent):  # event_type == 16
                self._force_commit = True
                continue

            for row in binlogevent.rows:
                if isinstance(binlogevent, DeleteRowsEvent):
                    if binlogevent.table == self.master:
                        rv = {
                            'action': 'delete',
                            'doc': row['values']
                        }
                    else:
                        rv = {
                            'action': 'update',
                            'doc': {k: row['values'][k] if self.id_key and self.id_key == k else None for k in row['values']}
                        }
                elif isinstance(binlogevent, UpdateRowsEvent):
                    rv = {
                        'action': 'update',
                        'doc': row['after_values']
                    }
                elif isinstance(binlogevent, WriteRowsEvent):
                    if binlogevent.table == self.master:
                        rv = {
                                'action': 'create',
                                'doc': row['values']
                            }
                    else:
                        rv = {
                                'action': 'update',
                                'doc': row['values']
                            }
                else:
                    logging.error('unknown action type in binlog')
                    raise TypeError('unknown action type in binlog')
                yield rv
                print(rv)
        stream.close()
        raise IOError('[FAILED] mysql connection closed')

    def _sync_from_binlog(self):
        logging.info("Start to sync binlog")
        docs = reduce(lambda x, y: y(x), [self._processor], self._binlog_loader())

    def start(self):
        try:
            self._sync_from_binlog()
        except Exception:
            import traceback
            logging.error(traceback.format_exc())
            raise

def run():
    instance = Main()
    instance.start()