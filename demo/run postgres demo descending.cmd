@dotnet build --debug .\EXBP.Dipren.Demo.Postgres\EXBP.Dipren.Demo.Postgres.csproj

@.\EXBP.Dipren.Demo.Postgres\bin\Debug\net6.0\EXBP.Dipren.Demo.Postgres.exe deploy --database "Host = localhost; Port = 5432; Database = postgres; User ID = postgres; Password = development" --size 100000
@.\EXBP.Dipren.Demo.Postgres\bin\Debug\net6.0\EXBP.Dipren.Demo.Postgres.exe schedule "cuboid-001" --database "Host = localhost; Port = 5432; Database = postgres; User ID = postgres; Password = development" --reverse
@.\EXBP.Dipren.Demo.Postgres\bin\Debug\net6.0\EXBP.Dipren.Demo.Postgres.exe process "cuboid-001" --database "Host = localhost; Port = 5432; Database = postgres; User ID = postgres; Password = development" --threads 7 --batch-size 128 --batch-timeout 00:00:02 --clock-drift 00:00:00 --reverse
