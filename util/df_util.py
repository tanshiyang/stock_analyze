def append_column(df, column_name):
    col_name = df.columns.tolist()
    col_name.append(column_name)
    df = df.reindex(columns=col_name)


def try_fetch_one(self):
    if self.fetchone() is None:
        return None
    return self.fetchone[0]

