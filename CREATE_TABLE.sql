-- Table: public.patients
DROP TABLE IF EXISTS public.patients;

CREATE TABLE IF NOT EXISTS public.patients
(
    headerrow character(1) COLLATE pg_catalog."default",
    customer_name character varying(255) COLLATE pg_catalog."default" NOT NULL,
    customer_id character varying(18) COLLATE pg_catalog."default" NOT NULL,
    customer_open_date character varying(8) COLLATE pg_catalog."default" NOT NULL,
    last_consulted_date character varying(8) COLLATE pg_catalog."default" NOT NULL,
    vaccination_type character(5) COLLATE pg_catalog."default",
    doctor_consulted character(255) COLLATE pg_catalog."default",
    state character(5) COLLATE pg_catalog."default",
    country character(5) COLLATE pg_catalog."default",
    dob character varying(8) COLLATE pg_catalog."default",
    active_customer character(1) COLLATE pg_catalog."default",
    CONSTRAINT patients_pkey PRIMARY KEY (customer_id)
)

TABLESPACE pg_default;
