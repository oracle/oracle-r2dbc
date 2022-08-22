package oracle.r2dbc.impl;

import io.r2dbc.spi.Type;

public final class UserDefinedType implements Type {
    private final Class sqlClassType;
    private final String typeName;

    public UserDefinedType(Class sqlClassType, String typeName) {
        this.sqlClassType = sqlClassType;
        this.typeName = typeName;
    }

    @Override
    public Class<?> getJavaType() {
        return sqlClassType;
    }

    @Override
    public String getName() {
        return typeName;
    }
}