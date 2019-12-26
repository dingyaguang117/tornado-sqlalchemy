from sqlalchemy import BigInteger, Column, String
from tornado.gen import coroutine
from tornado.ioloop import IOLoop
from tornado.options import define, options, parse_command_line
from tornado.web import Application, RequestHandler

from tornado_sqlalchemy import (
    SessionMixin,
    as_future,
    SQLAlchemy
)


db = SQLAlchemy()


class User(db.Model):
    __tablename__ = 'users'

    id = Column(BigInteger, primary_key=True)
    username = Column(String(255))


class Foo(db.Model):
    __bind_key__ = 'foo'
    __tablename__ = 'foo'

    id = Column(BigInteger, primary_key=True)
    foo = Column(String(255))



class SynchronousRequestHandler(SessionMixin, RequestHandler):
    def get(self):
        with self.make_session() as session:
            count = session.query(User).count()

        # OR count = self.session.query(User).count()

        self.write('{} users so far!'.format(count))


class GenCoroutinesRequestHandler(SessionMixin, RequestHandler):
    @coroutine
    def get(self):
        with self.make_session() as session:
            count = yield as_future(session.query(User).count)

        self.write('{} users so far!'.format(count))


class NativeCoroutinesRequestHandler(SessionMixin, RequestHandler):
    async def get(self):
        with self.make_session() as session:
            count = await as_future(session.query(User).count)

        self.write('{} users so far!'.format(count))


if __name__ == '__main__':

    app = Application(
        [
            (r'/sync', SynchronousRequestHandler),
            (r'/gen-coroutines', GenCoroutinesRequestHandler),
            (r'/native-coroutines', NativeCoroutinesRequestHandler),
        ],
        sqlalchemy_database_uri='mysql://t_sa:t_sa@localhost/t_sa',
        sqlalchemy_binds={
            'foo': 'mysql://t_sa:t_sa@localhost/t_sa_1',
            'bar': 'mysql://t_sa:t_sa@localhost/t_sa_2',
        }
    )
    db.init_app(app)

    db.create_all()

    session = db.Session()
    print(session)

    session.add(User(username='a'))
    session.commit()


    # db.Model.metadata.create_all(db.get_engine())
    # db.Model.metadata.create_all(db.get_engine('foo'))

    print('Listening on port 8888')

    app.listen(8888)
    IOLoop.current().start()
