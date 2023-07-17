from faker import Faker
import pandas as pd
import fastparquet as fp
fake = Faker()

# 더미 데이터 프레임 생성
df = pd.DataFrame({
    'name': [fake.name() for _ in range(10)],
    'email': [fake.email() for _ in range(10)],
    'phone_number': [fake.phone_number() for _ in range(10)],
    'job': [fake.job() for _ in range(10)],
    'address': [fake.address() for _ in range(10)]
})

# 더미 데이터 프레임 출력
print(df)

df.to_parquet('example.parquet')