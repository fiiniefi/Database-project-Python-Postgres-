CREATE EXTENSION pgcrypto;

CREATE TABLE Member(
	id integer PRIMARY KEY,
	password varchar(128) NOT NULL,
	rank varchar(30) NOT NULL DEFAULT 'regular',
    activity_date timestamp without time zone NOT NULL);

CREATE TABLE Project(
	id integer PRIMARY KEY,
	id_leader integer REFERENCES Member(id),
	type varchar(30) NOT NULL DEFAULT 'general',
	creation_date timestamp without time zone NOT NULL);

CREATE TABLE Action(
	id integer PRIMARY KEY,
	id_project integer REFERENCES Project(id) NOT NULL,
	id_member integer REFERENCES Member(id) NOT NULL,
	statement varchar NOT NULL,
	creation_date timestamp without time zone NOT NULL,
	upvotes integer DEFAULT 0,
	downvotes integer DEFAULT 0);

CREATE TABLE Vote(
	id_member integer REFERENCES Member(id) NOT NULL,
	id_action integer REFERENCES Action(id) NOT NULL,
	vote_decision varchar NOT NULL,
	creation_date timestamp without time zone NOT NULL);

CREATE SEQUENCE id_distribution;

SELECT setval('id_distribution', MAX(id)) FROM
        (SELECT id FROM member UNION
         SELECT id FROM action UNION
         SELECT id FROM project) joined_ids;

ALTER TABLE member ALTER COLUMN id SET DEFAULT nextval('id_distribution');

ALTER TABLE project ALTER COLUMN id SET DEFAULT nextval('id_distribution');

ALTER TABLE action ALTER COLUMN id SET DEFAULT nextval('id_distribution');

CREATE OR REPLACE FUNCTION update_votes()
RETURNS TRIGGER
AS $X$
BEGIN
IF NEW.vote_decision='up' THEN UPDATE action SET upvotes=upvotes+1 WHERE NEW.id_action=action.id; END IF;
IF NEW.vote_decision='down' THEN UPDATE action SET downvotes=downvotes+1 WHERE NEW.id_action=action.id; END IF;
RETURN NULL;
END
$X$ LANGUAGE plpgsql;

CREATE TRIGGER vote_updater
AFTER INSERT ON vote
FOR EACH ROW
EXECUTE PROCEDURE update_votes();
END;

CREATE OR REPLACE FUNCTION check_id()
RETURNS TRIGGER
AS $X$
BEGIN
IF EXISTS (SELECT id FROM member WHERE id=NEW.id UNION (SELECT id FROM action WHERE id=NEW.id) UNION
            (SELECT id FROM project WHERE id=NEW.id)) THEN RAISE EXCEPTION 'ID consistency violated with %', NEW.id;
ELSE RETURN NEW; END IF;
END
$X$ LANGUAGE plpgsql;

CREATE TRIGGER id_consistency
BEFORE INSERT ON member
FOR EACH ROW
EXECUTE PROCEDURE check_id();
END;

CREATE TRIGGER id_consistency
BEFORE INSERT ON action
FOR EACH ROW
EXECUTE PROCEDURE check_id();
END;

CREATE TRIGGER id_consistency
BEFORE INSERT ON project
FOR EACH ROW
EXECUTE PROCEDURE check_id();
END;


CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER
AS $X$
BEGIN
EXECUTE format('UPDATE member SET activity_date = $1.creation_date WHERE member.id=$1.%s', TG_ARGV[0]) USING NEW;
RETURN NEW;
END
$X$ LANGUAGE plpgsql;

CREATE TRIGGER date_updater
AFTER INSERT ON project
FOR EACH ROW
EXECUTE PROCEDURE update_timestamp("id_leader");
END;

CREATE TRIGGER date_updater
AFTER INSERT ON action
FOR EACH ROW
EXECUTE PROCEDURE update_timestamp("id_member");
END;

CREATE TRIGGER date_updater
AFTER INSERT ON vote
FOR EACH ROW
EXECUTE PROCEDURE update_timestamp("id_member");
END;
