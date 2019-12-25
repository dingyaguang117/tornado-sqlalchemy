import logging
import multiprocessing
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import contextmanager
from typing import Callable, Iterator, Optional

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from tornado.concurrent import Future, chain_future
from tornado.locks import Lock
from tornado.ioloop import IOLoop
from tornado.web import Application

__all__ = (
    'as_future',
    'SessionMixin',
    'set_max_workers',
    'SQLAlchemy'
)


class MissingFactoryError(Exception):
    pass


class MissingDatabaseSettingError(Exception):
    pass


class _AsyncExecution:
    """Tiny wrapper around ThreadPoolExecutor. This class is not meant to be
    instantiated externally, but internally we just use it as a wrapper around
    ThreadPoolExecutor so we can control the pool size and make the
    `as_future` function public.
    """

    def __init__(self, max_workers: Optional[int] = None):
        self._max_workers = (
            max_workers or multiprocessing.cpu_count()
        )  # type: int
        self._pool = None  # type: Optional[Executor]

    def set_max_workers(self, count: int):
        if self._pool:
            self._pool.shutdown(wait=True)

        self._max_workers = count
        self._pool = ThreadPoolExecutor(max_workers=self._max_workers)

    def as_future(self, query: Callable) -> Future:
        # concurrent.futures.Future is not compatible with the "new style"
        # asyncio Future, and awaiting on such "old-style" futures does not
        # work.
        #
        # tornado includes a `run_in_executor` function to help with this
        # problem, but it's only included in version 5+. Hence, we copy a
        # little bit of code here to handle this incompatibility.

        if not self._pool:
            self._pool = ThreadPoolExecutor(max_workers=self._max_workers)

        old_future = self._pool.submit(query)
        new_future = Future()  # type: Future

        IOLoop.current().add_future(
            old_future, lambda f: chain_future(f, new_future)
        )

        return new_future


class SessionMixin:
    _session = None  # type: Optional[Session]
    application = None  # type: Optional[Application]

    @contextmanager
    def make_session(self) -> Iterator[Session]:
        session = None

        try:
            session = self._make_session()

            yield session
        except Exception:
            if session:
                session.rollback()
            raise
        else:
            session.commit()
        finally:
            if session:
                session.close()

    def on_finish(self):
        next_on_finish = None

        try:
            next_on_finish = super(SessionMixin, self).on_finish
        except AttributeError:
            pass

        if self._session:
            self._session.commit()
            self._session.close()

        if next_on_finish:
            next_on_finish()

    @property
    def session(self) -> Session:
        if not self._session:
            self._session = self._make_session()
        return self._session

    def _make_session(self) -> Session:
        if not self.application:
            raise MissingFactoryError()
        return self.application.db.Session()


_async_exec = _AsyncExecution()

as_future = _async_exec.as_future

set_max_workers = _async_exec.set_max_workers


class SessionEx(Session):
    """The SessionEx extends the default session system with bind selection.
    """

    def __init__(self, db, autocommit=False, autoflush=True, **options):
        self.app = db.get_app()
        bind = options.pop('bind', None) or db.engine
        binds = options.pop('binds', db.get_binds())

        Session.__init__(
            self, autocommit=autocommit, autoflush=autoflush,
            bind=bind, binds=binds, **options
        )

    def get_bind(self, mapper=None, clause=None):
        """Return the engine or connection for a given model or
        table, using the ``__bind_key__`` if it is set.
        """
        # mapper is None if someone tries to just get a connection
        if mapper is not None:
            try:
                # SA >= 1.3
                persist_selectable = mapper.persist_selectable
            except AttributeError:
                # SA < 1.3
                persist_selectable = mapper.mapped_table

            info = getattr(persist_selectable, 'info', {})
            bind_key = info.get('bind_key')
            if bind_key is not None:
                return self.app.db.get_engine(bind=bind_key)
        return Session.get_bind(self, mapper, clause)


class BindMeta(DeclarativeMeta):
    def __init__(cls, name, bases, d):
        bind_key = (
            d.pop('__bind_key__', None)
            or getattr(cls, '__bind_key__', None)
        )

        super(BindMeta, cls).__init__(name, bases, d)

        if bind_key is not None and getattr(cls, '__table__', None) is not None:
            cls.__table__.info['bind_key'] = bind_key


class SQLAlchemy(object):

    def __init__(self, app=None, sesion_options=None, engine_options=None):

        self.Session = sessionmaker(class_=SessionEx, db=self, **(sesion_options or {}))
        self.Model = self.make_declarative_base()

        self._engine_options = engine_options or {}
        self._engine_lock = Lock()
        self._engines = {}

        if app is not None:
            self.init_app(app)
        else:
            self.app = None

    def init_app(self, app):
        if self.app:
            logging.warning('init_app called more than once, SQLALCHEMY_ENGINE_OPTIONS may not take effect')

        self.app = app
        self.app.db = self

        bind = app.settings.get('SQLALCHEMY_DATABASE_URI')
        binds = app.settings.get('SQLALCHEMY_BINDS')

        if not bind and not binds:
            raise MissingDatabaseSettingError()

        engine_options = app.settings.get('SQLALCHEMY_ENGINE_OPTIONS') or {}
        engine_options = engine_options.copy()
        engine_options.update(self._engine_options)
        self._engine_options = engine_options

    def get_app(self):
        if not self.app:
            raise RuntimeError('No application found. Please init_app first.')
        return self.app

    @property
    def engine(self):
        return self.get_engine()

    @property
    def metadata(self):
        return self.Model.metadata

    def create_engine(self, bind=None):
        app = self.get_app()

        if bind is None:
            uri = app.settings['SQLALCHEMY_DATABASE_URI']
        else:
            binds = app.settings.get('SQLALCHEMY_BINDS') or ()
            if bind not in binds:
                raise RuntimeError('bind {} undefined.'.format(bind))
            uri = binds[bind]

        options = self._engine_options
        return create_engine(uri, **options)

    def get_engine(self, bind=None):
        """Returns a specific engine. cached in self.engines """
        # with self._engine_lock:
        engine = self._engines.get(bind)

        if engine is None:
            engine = self.create_engine(bind)
            self._engines[bind] = engine

        return engine

    def get_tables_for_bind(self, bind=None):
        """Returns a list of all tables relevant for a bind."""
        result = []
        for table in self.Model.metadata.tables.values():
            if table.info.get('bind_key') == bind:
                result.append(table)
        return result

    def get_binds(self):
        """Returns a dictionary with a table->engine mapping.

        This is suitable for use of sessionmaker(binds=db.get_binds()).
        """
        app = self.get_app()
        binds = [None] + list(app.settings.get('SQLALCHEMY_BINDS') or ())
        retval = {}
        for bind in binds:
            engine = self.get_engine(bind)
            tables = self.get_tables_for_bind(bind)
            retval.update(dict((table, engine) for table in tables))
        return retval

    def _execute_for_all_tables(self, bind, operation, skip_tables=False):
        app = self.get_app()

        if bind == '__all__':
            binds = [None] + list(app.settings.get('SQLALCHEMY_BINDS') or ())
        elif isinstance(bind, str) or bind is None:
            binds = [bind]
        else:
            binds = bind

        for bind in binds:
            extra = {}
            if not skip_tables:
                tables = self.get_tables_for_bind(bind)
                extra['tables'] = tables
            op = getattr(self.Model.metadata, operation)
            op(bind=self.get_engine(bind), **extra)

    def create_all(self, bind='__all__'):
        """Creates all tables.
        """
        self._execute_for_all_tables(bind, 'create_all')

    def drop_all(self, bind='__all__'):
        """Drops all tables.
        """
        self._execute_for_all_tables(bind, 'drop_all')

    def reflect(self, bind='__all__'):
        """Reflects tables from the database.
        """
        self._execute_for_all_tables(bind, 'reflect', skip_tables=True)

    def make_declarative_base(self):
        base = declarative_base(metaclass=BindMeta)
        return base
