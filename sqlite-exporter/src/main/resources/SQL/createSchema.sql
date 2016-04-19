
create table @{tableName}(
    @foreach{ field : fields }@{field.originalName}
    @end{','}
);