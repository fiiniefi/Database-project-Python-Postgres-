import psycopg2
from datetime import datetime
import json
from .exceptions import DatabaseException, InvalidMember, InvalidRowCount
from functools import reduce


class PostgresAPI:
    STATUS_SUCCESS = "OK"
    STATUS_FAILURE = "ERROR"

    def __init__(self, database=None, user=None, password=None, host=None):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.connection = None
        self.cursor = None

    def open(self, database, user, password, host='localhost'):
        """
        Establishes connection with database using credentials given in parameters.
        Also constructs database with 'db_definition.sql' file from current project

        :param str database: database to open
        :param str user: user opening database
        :param str password: password to user's account
        :param str host: place where database server is hosted. Defaults to 'localhost'
        """
        self.connection = psycopg2.connect(database=database, user=user,
                                           password=password, host=host)
        self.cursor = self.connection.cursor()
        self.cursor.execute(open("db_definition.sql").read())

    def leader(self, timestamp, password, member):
        """
        Defines leader by given credentials and sets his last activity time to
        value given in 'timestamp' parameter

        :param int timestamp: creation date of new leader
        :param str password: password of new leader
        :param int member: id of new leader
        """
        timestamp = datetime.fromtimestamp(timestamp)
        try:
            self._create_member(member, password, 'leader', timestamp)
        except psycopg2.InternalError:
            return json.dumps({'status': self.STATUS_FAILURE})

    def support(self, timestamp, member, password, action, project, authority=None):
        """
        Defines support action for project id given in 'project' parameter

        :param int timestamp: action's creation date
        :param int member: id of member who proposes an action
        :param str password: member's password
        :param int action: new action's id
        :param int project: id of project to support
        :param int authority: id of leader who created project indicated in 'project' parameter (optional)
        :rtype: json
        :return: json containing status of request
        """
        return self._define_action(timestamp, member, password, action, project, 'support', authority)

    def protest(self, timestamp, member, password, action, project, authority=None):
        """
        Defines protest action for project id given in 'project' parameter

        :param int timestamp: action's creation date
        :param int member: id of member who proposes an action
        :param str password: member's password
        :param int action: new action's id
        :param int project: id of project to protest
        :param int authority: id of leader who created project indicated in 'project' parameter
        :rtype: json
        :return: json containing status of request
        """
        return self._define_action(timestamp, member, password, action, project, 'protest', authority)

    def upvote(self, timestamp, member, password, action):
        """
        Defines member's upvote for an action indicated in 'action' parameter.
        Single member can vote only once.

        :param int timestamp: creation date of a vote
        :param int member: id of voting member
        :param str password: password of voting member
        :param int action: id of action under voting
        :rtype: json
        :return: json containing status of request
        """
        return self._vote(timestamp, member, password, action, 'up')

    def downvote(self, timestamp, member, password, action):
        """
        Defines member's downvote for an action indicated in 'action' parameter.
        Single member can vote only once.

        :param int timestamp: creation date of a vote
        :param int member: id of voting member
        :param str password: password of voting member
        :param int action: id of action under voting
        :rtype: json
        :return: json containing status of request
        """
        return self._vote(timestamp, member, password, action, 'down')

    def actions(self, member, password, action_type=None, project=None, authority=None, **_):
        """
        Returns all actions in current state of the database

        :param int member: id of member who requests for actions
        :param str password: member's password
        :param str action_type: if passed, function returns all actions of given type (optional)
        :param int project: if passed, fucntion returns all actions for given project (optional)
        :param int authority: if passed, function returns all actions created by indicated authority (optional)
        :rtype: json
        :return: all actions in current state of the database
        """
        condition_payload = {'type': action_type, 'project.id': project, 'project.id_leader': authority}
        condition = self._generate_condition(**condition_payload)

        expr = "SELECT DISTINCT action.id, type, id_project, project.id_leader, upvotes, downvotes FROM action " \
               "LEFT JOIN vote ON (action.id=vote.id_action) " \
               "JOIN project ON (action.id_project=project.id) " \
               f"{condition} ORDER BY action.id"

        try:
            self._verify_leader(member, password)
            self.cursor.execute(expr)
            return json.dumps({'status': self.STATUS_SUCCESS, 'data': self.cursor.fetchall()})
        except psycopg2.InternalError:
            return json.dumps({'status': self.STATUS_FAILURE})

    def projects(self, member, password, authority=None, **_):
        """
        Returns all projects in current state of the database

        :param int member: id of member who requests for projects
        :param str password: member's password
        :param int authority: if passed, function returns all projects created by indicated authority (optional)
        :rtype: json
        :return: all projects in current state of the database
        """
        condition_payload = {'id_leader': authority}
        condition = self._generate_condition(**condition_payload)
        expr = f"SELECT DISTINCT id, id_leader FROM project {condition} ORDER BY id"

        try:
            self._verify_leader(member, password)
            self.cursor.execute(expr.format(condition))
            return json.dumps({'status': self.STATUS_SUCCESS, 'data': self.cursor.fetchall()})
        except psycopg2.InternalError:
            return json.dumps({'status': self.STATUS_FAILURE})

    def votes(self, member, password, action=None, project=None, **_):
        """
        Returns all votes in current state of the database

        :param int member: id of member who requests for projects
        :param str password: member's password
        :param int action: if passed, function returns all votes for indicated action (optional)
        :param int project: if passed, function returns all votes for actions
            defined for indicated project (optional)
        :rtype: json
        :return: all votes in current state of the database
        """
        condition_payload = {'vote.id_action': action, 'project.id': project}
        condition = self._generate_condition(**condition_payload)
        expr = "SELECT DISTINCT member.id, COALESCE(upvotes, 0), COALESCE(downvotes, 0) FROM member " \
               "LEFT JOIN (SELECT vote.id_member, upvotes, downvotes FROM vote " \
               "LEFT JOIN action ON (vote.id_action=action.id) " \
               f"JOIN project ON (action.id_project=project.id) {condition}) AS votes " \
               "ON (member.id=votes.id_member) " \
               "ORDER BY member.id"

        try:
            self._verify_leader(member, password)
            self.cursor.execute(expr.format(condition))
            return json.dumps({'status': self.STATUS_SUCCESS, 'data': self.cursor.fetchall()})
        except psycopg2.InternalError:
            return json.dumps({'status': self.STATUS_FAILURE})

    def trolls(self, timestamp):
        """
        Troll is a member who has more downvotes than upvotes for all summarized actions proposed by him.
        Returns all trolls in current state of the database

        :param int timestamp: current moment
        :rtype: json
        :return: all trolls in current state of the database
        """
        timestamp = datetime.fromtimestamp(timestamp)
        expr = "SELECT member.id, sum(upvotes), sum(downvotes), " \
               f"EXTRACT(YEAR FROM '{timestamp}'::timestamp without time zone) - " \
               "EXTRACT(YEAR FROM member.activity_date) <= 0 " \
               "FROM member " \
               "JOIN action ON (member.id=action.id_member) " \
               "WHERE downvotes - upvotes > 0 " \
               "GROUP BY member.id, upvotes, downvotes ORDER BY downvotes-upvotes DESC, member.id ASC"

        try:
            self.cursor.execute(expr)
            return json.dumps({'status': self.STATUS_SUCCESS, 'data': self.cursor.fetchall()})
        except psycopg2.InternalError:
            return json.dumps({'status': self.STATUS_FAILURE})

    def __enter__(self):
        self.open(self.database, self.user, self.password, self.host)
        return self

    def __del__(self):
        self.connection.close()

    def __exit__(self, *args):
        self.__del__()

    def _define_action(self, timestamp, member, password, action, project, statement, authority=None):
        timestamp = datetime.fromtimestamp(timestamp)
        expr = "INSERT INTO action VALUES " \
               f"({action}, {project}, {member}, '{statement}', '{timestamp}')"

        try:
            self._handle_member(member, password, timestamp)
            self._handle_project(project, authority, timestamp)
            self.cursor.execute(expr)
            return json.dumps({'status': self.STATUS_SUCCESS})
        except psycopg2.InternalError:
            return json.dumps({'status': self.STATUS_FAILURE})

    def _vote(self, timestamp, member, password, action, vote):
        timestamp = datetime.fromtimestamp(timestamp)
        expr = "INSERT INTO vote VALUES " \
               f"({member}, {action}, '{vote}', '{timestamp}')"

        try:
            self._handle_member(member, password, timestamp)
            self._row_existence_check('action', id=action)
            self.cursor.execute(expr)
            return json.dumps({'status': self.STATUS_SUCCESS})
        except psycopg2.InternalError:
            return json.dumps({'status': self.STATUS_FAILURE})

    def _handle_member(self, member, password, timestamp):
        try:
            self._row_existence_check('member', id=member)
            self._validate_member(member, password)
        except DatabaseException:
            self._create_member(member, password, 'regular', timestamp)

    def _handle_project(self, project, authority, timestamp):
        try:
            self._row_existence_check('project', id=project)
        except InvalidRowCount:
            if authority is None:
                raise InvalidMember("'authority' parameter should be passed if project is not defined")

            expr = "INSERT INTO project(id, id_leader, creation_date) VALUES " \
                   f"({project}, {authority}, '{timestamp}')"
            self.cursor.execute(expr)

    def _create_member(self, member, password, rank, timestamp):
        expr = "INSERT INTO member VALUES " \
               f"({member}, crypt('{password}', gen_salt('bf')), '{rank}', '{timestamp}')"
        self.cursor.execute(expr)

    def _validate_member(self, member, password):
        expr = f"SELECT * FROM member WHERE id={member} AND password=crypt('{password}', password)"
        self.cursor.execute(expr)
        if self.cursor.rowcount == 0 or datetime.now().year - self.cursor.fetchone()[-1].year != 0:
            raise InvalidMember("Authentication failed or member is frozen")

    def _verify_leader(self, member, password):
        self._validate_member(member, password)
        try:
            self._row_existence_check('member', id=member, rank='leader')
        except InvalidRowCount:
            raise InvalidMember("Wrong password or indicated member is not an active leader")

    def _row_existence_check(self, table, **kwargs):
        expr = f"SELECT * FROM {table} {self._generate_condition(**kwargs)}"
        self.cursor.execute(expr)
        if self.cursor.rowcount == 0:
            raise InvalidRowCount(f"No rows in '{table}' table containing specified parameters")

    def _verify_if_voted(self, member, action):
        expr = f"SELECT * FROM vote WHERE id_member={member} AND id_action={action}"
        self.cursor.execute(expr)
        if self.cursor.rowcount != 0:
            raise InvalidRowCount(f"Member {member} have already voted for action {action}")

    def _generate_condition(self, **kwargs):
        return reduce(lambda res, pair: res if pair[1] is None else res + f"AND {pair[0]}='{pair[1]}' ",
                      kwargs.items(),
                      "WHERE 1=1 ")

    def __print_table_state(self, table):
        self.cursor.execute(f"SELECT * FROM {table}")
        print(table, "  ", self.cursor.fetchall())
