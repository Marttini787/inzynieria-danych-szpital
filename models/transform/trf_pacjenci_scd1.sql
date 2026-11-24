select *
from {{ source("szpital_raw", "pacjenci") }}
qualify row_number() over (partition by id order by updated_at desc) = 1