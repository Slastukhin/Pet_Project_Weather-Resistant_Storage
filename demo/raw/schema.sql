--
-- PostgreSQL database dump
--


-- Dumped from database version 16.15 (Debian 16.15-1.pgdg13+2)
-- Dumped by pg_dump version 16.15 (Debian 16.15-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: raw; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA raw;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: cities; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.cities (
    city_slug text NOT NULL,
    city_name text NOT NULL,
    latitude numeric(9,5) NOT NULL,
    longitude numeric(9,5) NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: kaggle_coffee; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.kaggle_coffee (
    hour_of_day integer NOT NULL,
    cash_type text NOT NULL,
    money numeric(3,1) NOT NULL,
    coffee_name text NOT NULL,
    time_of_day text NOT NULL,
    weekday text NOT NULL,
    month_name text NOT NULL,
    weekdaysort integer NOT NULL,
    monthsort integer NOT NULL,
    date date NOT NULL,
    "time" time without time zone NOT NULL
);


--
-- Name: synthetic_coffee_sales; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.synthetic_coffee_sales (
    source_sale_id text,
    synthetic_sale_id text,
    synthetic_sale_item_id text,
    city_slug text,
    timezone text,
    date date,
    sold_at timestamp without time zone,
    product_name text,
    payment_type text,
    amount_money numeric(3,1),
    price_multiplier double precision,
    amount_rub numeric(12,2),
    quantity integer NOT NULL,
    line_number integer NOT NULL,
    is_synthetic boolean NOT NULL
);


--
-- Name: weather_archive; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.weather_archive (
    city_slug text NOT NULL,
    period_start date NOT NULL,
    period_end date NOT NULL,
    payload jsonb NOT NULL,
    loaded_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: weather_forecast; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.weather_forecast (
    city_slug text NOT NULL,
    issued_at timestamp with time zone NOT NULL,
    forecast_days integer NOT NULL,
    payload jsonb NOT NULL,
    loaded_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: cities cities_pkey; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.cities
    ADD CONSTRAINT cities_pkey PRIMARY KEY (city_slug);


--
-- Name: weather_archive weather_archive_pk; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.weather_archive
    ADD CONSTRAINT weather_archive_pk PRIMARY KEY (city_slug, period_start, period_end);


--
-- Name: weather_forecast weather_forecast_pk; Type: CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.weather_forecast
    ADD CONSTRAINT weather_forecast_pk PRIMARY KEY (city_slug, issued_at);


--
-- Name: weather_archive weather_archive_city_slug_fkey; Type: FK CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.weather_archive
    ADD CONSTRAINT weather_archive_city_slug_fkey FOREIGN KEY (city_slug) REFERENCES raw.cities(city_slug);


--
-- Name: weather_forecast weather_forecast_city_slug_fkey; Type: FK CONSTRAINT; Schema: raw; Owner: -
--

ALTER TABLE ONLY raw.weather_forecast
    ADD CONSTRAINT weather_forecast_city_slug_fkey FOREIGN KEY (city_slug) REFERENCES raw.cities(city_slug);


--
-- PostgreSQL database dump complete
--


