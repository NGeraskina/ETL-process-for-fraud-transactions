
CREATE TABLE public.nsgr_stg_terminals (
    terminal_id VARCHAR,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR
);

CREATE TABLE public.nsgr_stg_blacklist (
    passport_num VARCHAR PRIMARY KEY,
    entry_dt DATE
);

CREATE TABLE public.nsgr_stg_clients (
    client_id VARCHAR PRIMARY KEY,
    last_name VARCHAR,
    first_name VARCHAR,
    patrinymic VARCHAR,
    date_of_birth DATE,
    passport_num VARCHAR,
    passport_valid_to DATE,
    phone VARCHAR
);

CREATE TABLE public.nsgr_stg_accounts (
    account_num VARCHAR PRIMARY KEY,
    valid_to DATE,
    client VARCHAR,
    FOREIGN KEY (client) REFERENCES public.nsgr_stg_clients(client_id)
);

CREATE TABLE public.nsgr_stg_cards (
    card_num VARCHAR PRIMARY KEY,
    account_num VARCHAR,
    FOREIGN KEY (account_num) REFERENCES public.nsgr_stg_accounts(account_num)
);

CREATE TABLE public.nsgr_stg_transactions (
    trans_id VARCHAR PRIMARY KEY,
    trans_date TIMESTAMP,
    card_num VARCHAR,
    oper_type VARCHAR,
    amt DECIMAL,
    oper_result VARCHAR,
    terminal VARCHAR,
    FOREIGN KEY (card_num) REFERENCES public.nsgr_stg_cards(card_num),
    --FOREIGN KEY (terminal) REFERENCES public.nsgr_stg_terminals(terminal_id)
);


CREATE TABLE public.nsgr_dwh_dim_terminals_hist (
    terminal_id VARCHAR,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR,
    effective_from DATE, 
    effective_to DATE, 
    deleted_flg BOOL,
    PRIMARY KEY (terminal_id, terminal_type, terminal_city, terminal_address)
);

CREATE TABLE public.nsgr_dwh_fact_passport_blacklist (
    passport_num VARCHAR,
    entry_dt DATE,
    PRIMARY KEY (passport_num, entry_dt)
);

CREATE TABLE public.nsgr_dwh_dim_clients_hist (
    client_id VARCHAR PRIMARY KEY,
    last_name VARCHAR,
    first_name VARCHAR,
    patrinymic VARCHAR,
    date_of_birth DATE,
    passport_num VARCHAR,
    passport_valid_to DATE,
    phone VARCHAR,
    effective_from DATE, 
    effective_to DATE, 
    deleted_flg BOOL

);

CREATE TABLE public.nsgr_dwh_dim_accounts_hist (
    account_num VARCHAR PRIMARY KEY,
    valid_to DATE,
    client VARCHAR,
    effective_from DATE, 
    effective_to DATE, 
    deleted_flg BOOL,
    FOREIGN KEY (client) REFERENCES public.nsgr_dwh_dim_clients(client_id)
);

CREATE TABLE public.nsgr_dwh_dim_cards_hist (
    card_num VARCHAR PRIMARY KEY,
    account_num VARCHAR,
    effective_from DATE, 
    effective_to DATE, 
    deleted_flg BOOL,
    FOREIGN KEY (account_num) REFERENCES public.nsgr_dwh_dim_accounts(account_num)
);

CREATE TABLE public.nsgr_dwh_fact_transactions (
    trans_id VARCHAR PRIMARY KEY,
    trans_date TIMESTAMP,
    card_num VARCHAR,
    oper_type VARCHAR,
    amt DECIMAL,
    oper_result VARCHAR,
    terminal VARCHAR,
    FOREIGN KEY (card_num) REFERENCES public.nsgr_dwh_dim_cards(card_num),
    FOREIGN KEY (terminal) REFERENCES public.nsgr_dwh_dim_terminals(terminal_id)
);

CREATE TABLE public.nsgr_rep_fraud (
    event_dt TIMESTAMP,
    passport VARCHAR,
    fio VARCHAR,
    phone VARCHAR,
    event_type VARCHAR,
    report_dt DATE
);
