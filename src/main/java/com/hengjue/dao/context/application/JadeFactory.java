/*
 * Copyright 2009-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License i distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hengjue.dao.context.application;

import java.lang.reflect.Proxy;

import javax.sql.DataSource;

import org.springframework.util.ClassUtils;

import com.hengjue.dao.context.JadeInvocationHandler;
import com.hengjue.dao.dataAccess.DataAccessFactoryAdapter;
import com.hengjue.dao.dataAccess.DataSourceFactory;
import com.hengjue.dao.dataAccess.SimpleDataSourceFactory;
import com.hengjue.dao.rowMapper.DefaultRowMapperFactory;
import com.hengjue.dao.rowMapper.RowMapperFactory;
import com.hengjue.dao.statement.DAOConfig;
import com.hengjue.dao.statement.DAOMetaData;
import com.hengjue.dao.statement.DefaultInterpreterFactory;
import com.hengjue.dao.statement.Interpreter;
import com.hengjue.dao.statement.StatementWrapperProvider;
import com.hengjue.dao.statement.cached.CacheProvider;

/**
 * 
 * @author 王志亮 [qieqie.wang@gmail.com]
 * 
 */
//BUG: @SQL中的 :1.create_date应该抛出错误而非返回null
public class JadeFactory {

    private RowMapperFactory rowMapperFactory = new DefaultRowMapperFactory();

    private DefaultInterpreterFactory interpreterFactory = new DefaultInterpreterFactory();

    private DataAccessFactoryAdapter dataAccessFactory;

    private CacheProvider cacheProvider;

    // 可选的
    private StatementWrapperProvider statementWrapperProvider;

    public JadeFactory() {
    }

    public JadeFactory(DataSource defaultDataSource) {
        setDataSourceFactory(new SimpleDataSourceFactory(defaultDataSource));
    }

    public JadeFactory(DataSourceFactory dataSourceFactory) {
        setDataSourceFactory(dataSourceFactory);
    }

    public void setDataSourceFactory(DataSourceFactory dataSourceFactory) {
        this.dataAccessFactory = new DataAccessFactoryAdapter(dataSourceFactory);
    }

    public void setCacheProvider(CacheProvider cacheProvider) {
        this.cacheProvider = cacheProvider;
    }

    public DataSourceFactory getDataSourceFactory() {
        if (this.dataAccessFactory == null) {
            return null;
        }
        return this.dataAccessFactory.getDataSourceFactory();
    }

    public void setRowMapperFactory(RowMapperFactory rowMapperFactory) {
        this.rowMapperFactory = rowMapperFactory;
    }

    public StatementWrapperProvider getStatementWrapperProvider() {
        return statementWrapperProvider;
    }

    public void setStatementWrapperProvider(StatementWrapperProvider statementWrapperProvider) {
        this.statementWrapperProvider = statementWrapperProvider;
    }

    public void addInterpreter(Interpreter... interpreters) {
        for (Interpreter interpreter : interpreters) {
            interpreterFactory.addInterpreter(interpreter);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T create(Class<?> daoClass) {
        try {
            DAOConfig config = new DAOConfig(dataAccessFactory, rowMapperFactory,
                interpreterFactory, cacheProvider, statementWrapperProvider);
            DAOMetaData daoMetaData = new DAOMetaData(daoClass, config);
            JadeInvocationHandler handler = new JadeInvocationHandler(daoMetaData);
            ClassLoader classLoader = ClassUtils.getDefaultClassLoader();
            return (T) Proxy.newProxyInstance(classLoader, new Class[] { daoClass }, handler);
        } catch (RuntimeException e) {
            throw new IllegalStateException("failed to create bean for " + daoClass.getName(), e);
        }
    }
}
