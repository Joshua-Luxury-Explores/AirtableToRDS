CREATE TABLE IF NOT EXISTS public.etl_sync_state (
    table_name TEXT PRIMARY KEY,
    last_loaded_at TIMESTAMPTZ NOT NULL
);

GRANT SELECT, INSERT, UPDATE ON public.etl_sync_state TO glue_user;

INSERT INTO public.etl_sync_state (table_name, last_loaded_at) VALUES
    ('scrapped_hotels', '2026-09-21T00:00:00+00:00'),
    ('room_category', '2026-09-21T00:00:00+00:00')
ON CONFLICT (table_name) DO UPDATE SET last_loaded_at = EXCLUDED.last_loaded_at;
