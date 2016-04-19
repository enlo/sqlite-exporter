insert into @{tableName} (
    @foreach{ field : fields }@{field.originalName}@end{','}
) values (
     @foreach{ field : fields } ? @end{','}
)
