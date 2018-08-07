-- company table & sequence
CREATE TABLE company (
    id bigint NOT NULL CONSTRAINT company_pkey PRIMARY KEY,
    name text NOT NULL,
    address text NOT NULL,
    primary_contact bigint,
    pricing_details bigint NOT NULL
);

CREATE SEQUENCE company_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE company_id_seq OWNED BY company.id;

ALTER TABLE ONLY company ALTER COLUMN id SET DEFAULT nextval('company_id_seq'::regclass);


-- company_partnership table & sequence
CREATE TABLE company_partnership (
    id bigint NOT NULL CONSTRAINT company_partnership_pkey PRIMARY KEY,
    company_a bigint NOT NULL,
    company_b bigint NOT NULL
);

CREATE SEQUENCE company_partnership_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE company_partnership_id_seq OWNED BY company_partnership.id;

ALTER TABLE ONLY company_partnership ALTER COLUMN id SET DEFAULT nextval('company_partnership_id_seq'::regclass);


-- customer table & sequence
CREATE TABLE customer (
    id bigint NOT NULL CONSTRAINT customer_pkey PRIMARY KEY,
    first_name text NOT NULL,
    last_name text NOT NULL,
    company_id bigint NOT NULL,
    out_of_office_delegate bigint,
    pricing_details bigint NOT NULL
);

CREATE SEQUENCE customer_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE customer_id_seq OWNED BY customer.id;

ALTER TABLE ONLY customer ALTER COLUMN id SET DEFAULT nextval('customer_id_seq'::regclass);


-- discount_rate table & sequence
CREATE TABLE discount_rate (
    id bigint NOT NULL CONSTRAINT discount_rate_pkey PRIMARY KEY,
    description text NOT NULL,
    value double precision NOT NULL
);

CREATE SEQUENCE discount_rate_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE discount_rate_id_seq OWNED BY discount_rate.id;

ALTER TABLE ONLY discount_rate ALTER COLUMN id SET DEFAULT nextval('discount_rate_id_seq'::regclass);


-- payment_method table & sequence
CREATE TABLE payment_method (
    id bigint NOT NULL CONSTRAINT payment_method_pkey PRIMARY KEY,
    description text NOT NULL,
    charge double precision NOT NULL
);

CREATE SEQUENCE payment_method_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE payment_method_id_seq OWNED BY payment_method.id;

ALTER TABLE ONLY payment_method ALTER COLUMN id SET DEFAULT nextval('payment_method_id_seq'::regclass);


-- pricing_details table & sequence
CREATE TABLE pricing_details (
    id bigint NOT NULL CONSTRAINT pricing_details_pkey PRIMARY KEY,
    description text NOT NULL,
    vat_rate bigint NOT NULL,
    discount_rate bigint NOT NULL,
    preferred_payment_method bigint NOT NULL
);

CREATE SEQUENCE pricing_details_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE pricing_details_id_seq OWNED BY pricing_details.id;

ALTER TABLE ONLY pricing_details ALTER COLUMN id SET DEFAULT nextval('pricing_details_id_seq'::regclass);


-- vat_rate table & sequence
CREATE TABLE vat_rate (
    id bigint NOT NULL CONSTRAINT vat_rate_pkey PRIMARY KEY,
    description text NOT NULL,
    value double precision NOT NULL
);

CREATE SEQUENCE vat_rate_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE vat_rate_id_seq OWNED BY vat_rate.id;

ALTER TABLE ONLY vat_rate ALTER COLUMN id SET DEFAULT nextval('vat_rate_id_seq'::regclass);