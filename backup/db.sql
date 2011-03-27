--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

--
-- Name: plpgsql; Type: PROCEDURAL LANGUAGE; Schema: -; Owner: arthur
--

CREATE OR REPLACE PROCEDURAL LANGUAGE plpgsql;


ALTER PROCEDURAL LANGUAGE plpgsql OWNER TO arthur;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: hosts; Type: TABLE; Schema: public; Owner: arthur; Tablespace: 
--

CREATE TABLE hosts (
    hostname character varying(255) NOT NULL,
    fetched_at timestamp without time zone
);


ALTER TABLE public.hosts OWNER TO arthur;

--
-- Name: links; Type: TABLE; Schema: public; Owner: arthur; Tablespace: 
--

CREATE TABLE links (
    hostname character varying(255),
    url character varying(2083) NOT NULL,
    headers text,
    body text,
    fetched_at timestamp without time zone,
    status smallint,
    scheduled_at timestamp without time zone
);


ALTER TABLE public.links OWNER TO arthur;

--
-- Name: hosts_pk; Type: CONSTRAINT; Schema: public; Owner: arthur; Tablespace: 
--

ALTER TABLE ONLY hosts
    ADD CONSTRAINT hosts_pk PRIMARY KEY (hostname);


--
-- Name: links_pk; Type: CONSTRAINT; Schema: public; Owner: arthur; Tablespace: 
--

ALTER TABLE ONLY links
    ADD CONSTRAINT links_pk PRIMARY KEY (url);


--
-- Name: links_fk; Type: FK CONSTRAINT; Schema: public; Owner: arthur
--

ALTER TABLE ONLY links
    ADD CONSTRAINT links_fk FOREIGN KEY (hostname) REFERENCES hosts(hostname) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- Name: public; Type: ACL; Schema: -; Owner: arthur
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM arthur;
GRANT ALL ON SCHEMA public TO arthur;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

