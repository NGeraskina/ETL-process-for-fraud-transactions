import os
import pandas as pd
import psycopg2
import re
import shutil


def list_unique_dates(dir):
    date_pattern = re.compile(r'(\d{8})')

    unique_dates = set()
    for filename in os.listdir(dir):
        match = date_pattern.search(filename)
        if match:
            unique_dates.add(match.group(1))

    return unique_dates


def files_to_archive(dir, date):
    date_pattern = re.compile(r'(\d{8})')
    archive_dir = os.path.join(dir, 'archive')
    for filename in os.listdir(dir):
        match = date_pattern.search(filename).group(1) if date_pattern.search(filename) else None
        if match == date:
            file_path = os.path.join(dir, filename)
            new_filename = filename + '.backup'
            new_file_path = os.path.join(archive_dir, new_filename)
        
            shutil.move(file_path, new_file_path)


unique_dates = sorted(list_unique_dates('./'))

# Создание подключения к PostgreSQL
conn = psycopg2.connect(database = "db",
                        host =     "rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net",
                        user =     "hseguest",
                        password = "hsepassword",
                        port =     "6432")

# Отключение автокоммита
conn.autocommit = False

# Создание курсора
cursor = conn.cursor()

for date in unique_dates:
    print(date)

    # очистка стейджинга
    try:
        for table in ('transactions', 'clients', 'cards', 'blacklist', 'terminals', 'accounts'):
            cursor.execute(f"TRUNCATE TABLE public.nsgr_stg_{table} CASCADE")
            conn.commit()

        print("Стейджинг успешно очищен")

    except Exception as error:
        print("Ошибка при выполнении операции:", error)


    ####################################################

    cursor.execute( """INSERT INTO public.nsgr_stg_clients (client_id, last_name, first_name, patrinymic, date_of_birth, passport_num, passport_valid_to, phone )
                        SELECT 
                            client_id, 
                            last_name,
                            first_name,
                            patronymic as patrinymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone 
                        FROM info.clients""" )
    conn.commit()
    print("public.nsgr_stg_clients загружена")

    cursor.execute( """INSERT INTO public.nsgr_stg_accounts (account_num, valid_to, client )
                        SELECT 
                            account as account_num, 
                            valid_to, 
                            client 
                        FROM info.accounts""" )
    conn.commit()
    print("public.nsgr_stg_accounts загружена")

    cursor.execute( """INSERT INTO public.nsgr_stg_cards (card_num, account_num)
                        SELECT 
                            card_num, 
                            account as account_num 
                        FROM info.cards""" )
    conn.commit()
    print("public.nsgr_stg_cards загружена")

    # Чтение из файла

    df = pd.read_excel(f'terminals_{date}.xlsx', sheet_name='terminals', header=0, index_col=None )
    cursor.executemany( "INSERT INTO public.nsgr_stg_terminals(terminal_id, terminal_type, terminal_city, terminal_address) VALUES( %s, %s, %s, %s )", df.values.tolist() )
    print("public.nsgr_stg_terminals загружена")


    df = pd.read_csv(f'transactions_{date}.txt', header=0, sep = ';', decimal= ",")

    columns = ['transaction_id', 'transaction_date', 'card_num', 'oper_type', 'amount', 'oper_result', 'terminal']
    # Запись DataFrame в таблицу базы данных
    cursor.executemany( "INSERT INTO public.nsgr_stg_transactions(trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal) VALUES( %s, %s, %s, %s, %s, %s, %s )", df[columns].values.tolist() )
    print("public.nsgr_stg_transactions загружена")


    df = pd.read_excel(f'passport_blacklist_{date}.xlsx', sheet_name='blacklist', header=0, index_col=None )
    columns = ['passport', 'date']
    cursor.executemany( "INSERT INTO public.nsgr_stg_blacklist( passport_num, entry_dt ) VALUES( %s, %s )", df[columns].values.tolist() )
    print("public.nsgr_stg_blacklist загружена")

    # складываем файлы в бекап
    files_to_archive('./', date)
    print('файлы в бекапе')


    cursor.execute(f"""
    DO $$
    BEGIN
    -- Проверяем, пустая ли таблица
    IF NOT EXISTS (SELECT 1 FROM public.nsgr_dwh_dim_clients) THEN
        -- Таблица пустая, вставляем все записи из stg
        INSERT INTO public.nsgr_dwh_dim_clients (client_id, last_name, first_name, patrinymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
        SELECT 
            client_id,
            last_name,
            first_name,
            patrinymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            to_date('{date}', 'DDMMYYYY') as effective_from, 
            '3000-01-01'::DATE as effective_to, 
            FALSE as deleted_flg
        FROM public.nsgr_stg_clients AS s;
    ELSE
        UPDATE public.nsgr_dwh_dim_clients
        SET effective_to = to_date('{date}', 'DDMMYYYY'), deleted_flg = TRUE
        WHERE (client_id, last_name, first_name, patrinymic, date_of_birth, passport_num, passport_valid_to, phone)
        IN (
            SELECT d.client_id, d.last_name, d.first_name, d.patrinymic, d.date_of_birth, d.passport_num, d.passport_valid_to, d.phone
            FROM public.nsgr_dwh_dim_clients d
            JOIN public.nsgr_stg_clients s
            ON d.client_id = s.client_id
            WHERE 
                (   d.last_name      <> s.last_name OR
                    d.first_name     <> s.first_name OR
                    d.patrinymic     <> s.patrinymic OR
                    d.date_of_birth  <> s.date_of_birth OR
                    d.passport_num   <> s.passport_num OR
                    d.passport_valid_to <> s.passport_valid_to OR
                    d.phone          <> s.phone
                ) 
                AND d.effective_to = '3000-01-01'::DATE
        );
                
        INSERT INTO public.nsgr_dwh_dim_clients (client_id, last_name, first_name, patrinymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
        SELECT 
            s.client_id as client_id, 
            s.last_name as last_name, 
            s.first_name as first_name, 
            s.patrinymic as patrinymic, 
            s.date_of_birth as date_of_birth, 
            s.passport_num as passport_num, 
            s.passport_valid_to, 
            s.phone as phone, 
            to_date('{date}', 'DDMMYYYY') as effective_from, 
            '3000-01-01'::DATE as effective_to, 
            FALSE as deleted_flg
        FROM 
            public.nsgr_stg_clients s
        LEFT JOIN 
            public.nsgr_dwh_dim_clients d ON s.client_id = d.client_id 
                AND d.effective_to = '3000-01-01'::DATE
        WHERE 
            d.client_id IS NULL OR ( 
                d.client_id IS NOT NULL AND (
                    d.last_name      <> s.last_name OR
                    d.first_name     <> s.first_name OR
                    d.patrinymic     <> s.patrinymic OR
                    d.date_of_birth  <> s.date_of_birth OR
                    d.passport_num   <> s.passport_num OR
                    d.passport_valid_to <> s.passport_valid_to OR
                    d.phone          <> s.phone
                )
            );
    END IF;
    END $$;
    ;

    """)
    conn.commit()
    print("public.nsgr_stg_clients отгружена в public.nsgr_dwh_dim_clients")


    cursor.execute(f"""
    DO $$
    BEGIN
    -- Проверяем, пустая ли таблица
    IF NOT EXISTS (SELECT 1 FROM public.nsgr_dwh_dim_accounts) THEN
        -- Таблица пустая, вставляем все записи из stg
        INSERT INTO public.nsgr_dwh_dim_accounts (account_num, valid_to, client, effective_from, effective_to, deleted_flg)
        SELECT 
            account_num,
            valid_to,
            client,
            to_date('{date}', 'DDMMYYYY') as effective_from, 
            '3000-01-01'::DATE as effective_to, 
            FALSE as deleted_flg
        FROM public.nsgr_stg_accounts AS s;
    ELSE
        UPDATE public.nsgr_dwh_dim_accounts
        SET effective_to = to_date('{date}', 'DDMMYYYY'), deleted_flg = TRUE
        WHERE (account_num, valid_to, client)
        IN (
            SELECT d.account_num, d.valid_to, d.client
            FROM public.nsgr_dwh_dim_accounts d
            JOIN public.nsgr_stg_accounts s
            ON d.account_num = s.account_num
            WHERE 
                (   d.valid_to <> s.valid_to OR
                    d.client <> s.client
                ) 
                AND d.effective_to = '3000-01-01'::DATE
        );
                
        INSERT INTO public.nsgr_dwh_dim_accounts (account_num, valid_to, client, effective_from, effective_to, deleted_flg)
        SELECT 
            s.account_num as account_num, 
            s.valid_to as valid_to, 
            s.client as client, 
            to_date('{date}', 'DDMMYYYY') as effective_from, 
            '3000-01-01'::DATE as effective_to, 
            FALSE as deleted_flg
        FROM 
            public.nsgr_stg_accounts s
        LEFT JOIN 
            public.nsgr_dwh_dim_accounts d ON s.account_num = d.account_num 
                AND d.effective_to = '3000-01-01'::DATE
        WHERE 
            d.account_num IS NULL OR 
                (   d.valid_to <> s.valid_to OR
                    d.client <> s.client
                ) 
            ;
    END IF;
    END $$;
    ;

    """)
    conn.commit()
    print("public.nsgr_stg_accounts отгружена в public.nsgr_dwh_dim_accounts")


    cursor.execute(f"""
    DO $$
    BEGIN
    -- Проверяем, пустая ли таблица
    IF NOT EXISTS (SELECT 1 FROM public.nsgr_dwh_dim_cards) THEN
        -- Таблица пустая, вставляем все записи из stg
        INSERT INTO public.nsgr_dwh_dim_cards (card_num, account_num, effective_from, effective_to, deleted_flg)
        SELECT 
            card_num,
            account_num,
            to_date('{date}', 'DDMMYYYY') as effective_from, 
            '3000-01-01'::DATE as effective_to, 
            FALSE as deleted_flg
        FROM public.nsgr_stg_cards AS s;
    ELSE
        UPDATE public.nsgr_dwh_dim_cards
        SET effective_to = to_date('{date}', 'DDMMYYYY'), deleted_flg = TRUE
        WHERE (card_num, account_num)
        IN (
            SELECT d.card_num, d.account_num
            FROM public.nsgr_dwh_dim_cards d
            JOIN public.nsgr_stg_cards s
            ON d.card_num = s.card_num
            WHERE 
                (   
                    d.account_num <> s.account_num
                ) 
                AND d.effective_to = '3000-01-01'::DATE
        );
                
        INSERT INTO public.nsgr_dwh_dim_cards (card_num, account_num, effective_from, effective_to, deleted_flg)
        SELECT 
            s.card_num as card_num, 
            s.account_num as account_num, 
            to_date('{date}', 'DDMMYYYY') as effective_from, 
            '3000-01-01'::DATE as effective_to, 
            FALSE as deleted_flg
        FROM 
            public.nsgr_stg_cards s
        LEFT JOIN 
            public.nsgr_dwh_dim_cards d ON s.card_num = d.card_num 
                AND d.effective_to = '3000-01-01'::DATE
        WHERE 
            d.account_num IS NULL OR 
                (   
                    d.account_num <> s.account_num
                ) 
            ;
    END IF;
    END $$;
    ;

    """)
    conn.commit()
    print("public.nsgr_stg_cards отгружена в public.nsgr_dwh_dim_cards")


    cursor.execute(f"""
    DO $$
    BEGIN
    -- Проверяем, пустая ли таблица
    IF NOT EXISTS (SELECT 1 FROM public.nsgr_dwh_dim_terminals) THEN
        -- Таблица пустая, вставляем все записи из stg
        INSERT INTO public.nsgr_dwh_dim_terminals (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
        SELECT 
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            to_date('{date}', 'DDMMYYYY') as effective_from, 
            '3000-01-01'::DATE as effective_to, 
            FALSE as deleted_flg
        FROM public.nsgr_stg_terminals AS s;
    ELSE
        UPDATE public.nsgr_dwh_dim_terminals
        SET effective_to = to_date('{date}', 'DDMMYYYY'), deleted_flg = TRUE
        WHERE (terminal_id, terminal_type, terminal_city, terminal_address)
        IN (
            SELECT d.terminal_id, d.terminal_type, d.terminal_city, d.terminal_address
            FROM public.nsgr_dwh_dim_terminals d
            JOIN public.nsgr_stg_terminals s
            ON d.terminal_id = s.terminal_id
            WHERE 
                (   
                    d.terminal_type <> s.terminal_type OR
                    d.terminal_city <> s.terminal_city OR
                    d.terminal_address <> s.terminal_address
                ) 
                AND d.effective_to = '3000-01-01'::DATE
        );
                
        INSERT INTO public.nsgr_dwh_dim_terminals (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
        SELECT 
            s.terminal_id as terminal_id, 
            s.terminal_type as terminal_type, 
            s.terminal_city as terminal_city,
            s.terminal_address as terminal_address,
            to_date('{date}', 'DDMMYYYY') as effective_from, 
            '3000-01-01'::DATE as effective_to, 
            FALSE as deleted_flg
        FROM 
            public.nsgr_stg_terminals s
        LEFT JOIN 
            public.nsgr_dwh_dim_terminals d ON s.terminal_id = d.terminal_id 
                AND d.effective_to = '3000-01-01'::DATE
        WHERE 
            d.terminal_id IS NULL OR 
                (   
                    d.terminal_type <> s.terminal_type OR
                    d.terminal_city <> s.terminal_city OR
                    d.terminal_address <> s.terminal_address
                ) 
            ;
    END IF;
    END $$;
    ;

    """)
    conn.commit()
    print("public.nsgr_stg_terminals отгружена в public.nsgr_dwh_dim_terminals")


    cursor.execute(f"""
        insert into public.nsgr_dwh_fact_passport_blacklist (passport_num, entry_dt)
        select 
            s.passport_num as passport_num,
            s.entry_dt as entry_dt
        from public.nsgr_stg_blacklist as s
        LEFT JOIN 
            public.nsgr_dwh_fact_passport_blacklist d ON s.passport_num = d.passport_num 
        WHERE 
            d.passport_num IS NULL 
    ;
    """)
    conn.commit()
    print("public.nsgr_stg_blacklist  отгружена в public.nsgr_dwh_fact_passport_blacklist ")

    cursor.execute(f"""
        insert into public.nsgr_dwh_fact_transactions (trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal)
        select 
            trans_id,
            trans_date, 
            card_num, 
            oper_type, 
            amt, 
            oper_result, 
            terminal
        from public.nsgr_stg_transactions;
    """)
    conn.commit()
    print("public.nsgr_stg_transactions  отгружена в public.nsgr_dwh_fact_transactions ")


    # Делаем отчет
    cursor.execute(f"""
    WITH city_of_transactions AS (
    SELECT
        t.card_num as card_num,
        t.trans_id as trans_id,
        t.trans_date as trans_date,
        term.terminal_city as terminal_city,
        c.client_id as client_id
    FROM public.nsgr_dwh_fact_transactions as t
    JOIN public.nsgr_dwh_dim_cards as car on car.card_num = t.card_num 
    JOIN public.nsgr_dwh_dim_accounts as acc on acc.account_num = car.account_num  
    JOIN public.nsgr_dwh_dim_clients as c on c.client_id = acc.client 
    JOIN public.nsgr_dwh_dim_terminals as term ON t.terminal = term.terminal_id
    ),
    one_hour_diff_city_transactions AS (
        SELECT
            distinct 
            a.trans_id as trans_id, 
            a.client_id as client_id
        FROM city_of_transactions as a
        JOIN city_of_transactions as b ON a.card_num = b.card_num
        where a.terminal_city <> b.terminal_city AND abs(extract(epoch from a.trans_date - b.trans_date)) <= 60*60
    ),
    req_start_for_4_rule as (
    select 
        * 
    from public.nsgr_dwh_fact_transactions  as t
    join public.nsgr_dwh_dim_cards  as c on c.card_num = t.card_num
    join public.nsgr_dwh_dim_accounts as a on a.account_num  = c.account_num  
    join public.nsgr_dwh_dim_clients as cl on cl.client_id  = a.client),
    joined_req as (
    select 
        r1.client_id as client_id,
        r1.passport_num as passport_num,
        r1.trans_id as trans_id_start, 
        r2.trans_id as trans_id_20min, 
        r1.amt as amt_start, 
        r2.amt as amt_20min, 
        r1.trans_date as  trans_date_start,
        r2.trans_date as trans_date_20min,
        r2.oper_result as oper_result
    from req_start_for_4_rule as r1
    join req_start_for_4_rule as r2 on r2.client_id = r1.client_id and r2.trans_date between r1.trans_date and (r1.trans_date+(20 * interval '1 minute'))
    ),
    selected_req as (
    select 
        client_id,
        trans_id_start
    from joined_req
    group by 
        client_id,
        trans_id_start
    having count(*)>3 and 'SUCCESS'=ANY(array_agg(oper_result)) and 'REJECT'=ANY(array_agg(oper_result))
    ),
    sorted_req as (
    select 
    j.*,
    row_number() over(partition by j.client_id, j.trans_id_start order by trans_date_20min asc) as rn_datetime
    from joined_req as j
    join selected_req as s on s.client_id = j.client_id and j.trans_id_start = s.trans_id_start
    ),
    sorted_4w_only as (
    select 
    *, 
    row_number() over(partition by client_id, trans_id_start order by amt_20min desc) as rn_amt
    from sorted_req
    where rn_datetime in (1,2,3,4)
    order by client_id, trans_id_start, rn_datetime
    ),
    only_sorted_trans_amt as (
    select 
        client_id,
        trans_id_start
    from sorted_4w_only
    where rn_amt = rn_datetime
    group by 
        client_id,
        trans_id_start
    having count(*)=4
    ),
    fraud_start_here as (
    select 
        s.client_id,
        s.trans_id_start
    from sorted_4w_only as s
    join only_sorted_trans_amt as a on s.client_id = a.client_id and s.trans_id_start = a.trans_id_start
    where (s.rn_datetime = 4 and s.oper_result = 'SUCCESS')
    intersect
    select 
        s.client_id,
        s.trans_id_start
    from sorted_4w_only as s
    join only_sorted_trans_amt as a on s.client_id = a.client_id and s.trans_id_start = a.trans_id_start
    where (s.rn_datetime = 3 and s.oper_result = 'REJECT')
    intersect
    select 
        s.client_id,
        s.trans_id_start 
    from sorted_4w_only as s
    join only_sorted_trans_amt as a on s.client_id = a.client_id and s.trans_id_start = a.trans_id_start
    where (s.rn_datetime = 2 and s.oper_result = 'REJECT')
    intersect
    select 
        s.client_id,
        s.trans_id_start 
    from sorted_4w_only as s
    join only_sorted_trans_amt as a on s.client_id = a.client_id and s.trans_id_start = a.trans_id_start
    where (s.rn_datetime = 1 and s.oper_result = 'REJECT')
    ),
    reassembly_of_amounts AS (
    select
        f.client_id as client_id,
        f.trans_id_start as trans_id
    from sorted_4w_only as w
    join fraud_start_here as f on f.client_id = w.client_id and f.trans_id_start = w.trans_id_start
    where rn_datetime = 4
    )
    insert into public.nsgr_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
    select 
        t.trans_date as event_dt,
        c.passport_num as passport,
        c.first_name || ' ' || c.patrinymic || ' ' || c.last_name  as fio,
        c.phone as phone,
        case when b.passport_num is not null then 'Заблокированный паспорт' 
            when c.passport_valid_to < t.trans_date then 'Просроченный паспорт'
            when acc.valid_to < t.trans_date then 'Недействующий договор'
            when h.client_id is not null then 'Транзакции из разных городов за короткое время'
            when r.client_id is not null then 'Попытка перебора транзакций'
        end as event_type,
        to_date('{date}', 'DDMMYYYY') as report_dt
    from public.nsgr_dwh_fact_transactions as t
    left join public.nsgr_dwh_dim_cards as car on car.card_num = t.card_num 
    left join public.nsgr_dwh_dim_accounts as acc on acc.account_num = car.account_num  
    left join public.nsgr_dwh_dim_clients as c on c.client_id = acc.client 
    left join public.nsgr_dwh_fact_passport_blacklist as b on c.passport_num = b.passport_num 
    left join one_hour_diff_city_transactions as h on h.client_id = c.client_id and t.trans_id = h.trans_id
    LEFT JOIN reassembly_of_amounts AS r ON r.client_id = c.client_id and t.trans_id = r.trans_id
    where 
        c.deleted_flg = false and 
        (
            b.passport_num is not null or 
            c.passport_valid_to < t.trans_date or 
            acc.valid_to < t.trans_date or 
            (h.client_id is not null) or 
            (r.client_id is not null)
        )

    """)
    conn.commit()
    print(f"отчет отгружен за дату {date}")



# Закрываем соединение
cursor.close()
conn.close()

print("Подключение закрыто")
