from unittest import TestCase, mock

from tornado.testing import AsyncTestCase, gen_test
from tornado_sqlalchemy import as_future

from ._common import db, User, mysql_url


class EngineOptionTestCase(TestCase):

    def setUp(self) -> None:
        self.application = mock.Mock()
        self.application.settings = {
            'sqlalchemy_database_uri': mysql_url,
            'sqlalchemy_engine_options': {
                'echo': True,
                'pool_size': 1,
                'pool_timeout': 1
            },
        }
        db.init_app(self.application)
        db.create_all()

    def tearDown(self) -> None:
        db.drop_all()

    def test_echo(self):
        session = db.Session()
        count = session.query(User).count()
        print(count)
        session.close()


#
# class ConcurrencyTestCase(AsyncTestCase):
#     session_count = 5
#     sleep_duration = 5
#
#     @gen_test
#     def test_concurrent_requests_using_yield(self):
#         factory = make_session_factory(postgres_url, pool_size=1)
#
#         sessions = [factory.make_session() for _ in range(self.session_count)]
#
#         yield [
#             as_future(
#                 lambda: session.execute(
#                     'SELECT pg_sleep(:duration)',
#                     {'duration': self.sleep_duration},
#                 )
#             )
#             for session in sessions
#         ]
#
#         for session in sessions:
#             session.close()
#
#     @gen_test
#     async def test_concurrent_requests_using_async(self):
#         factory = make_session_factory(postgres_url, pool_size=1)
#
#         sessions = [factory.make_session() for _ in range(self.session_count)]
#
#         for session in sessions:
#             await as_future(
#                 lambda: session.execute(
#                     'SELECT pg_sleep(:duration)',
#                     {'duration': self.sleep_duration},
#                 )
#             )
#
#         for session in sessions:
#             session.close()
