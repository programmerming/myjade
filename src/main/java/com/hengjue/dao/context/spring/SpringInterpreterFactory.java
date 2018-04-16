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
package com.hengjue.dao.context.spring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.hengjue.dao.statement.DefaultInterpreterFactory;
import com.hengjue.dao.statement.Interpreter;
import com.hengjue.dao.statement.InterpreterComparator;
import com.hengjue.dao.statement.InterpreterFactory;
import com.hengjue.dao.statement.StatementMetaData;

/**
 * 
 * @author 王志亮 [qieqie.wang@gmail.com]
 * 
 */
public class SpringInterpreterFactory implements InterpreterFactory, ApplicationContextAware {

    private DefaultInterpreterFactory interpreterFactory;

    private ListableBeanFactory beanFactory;

    public SpringInterpreterFactory() {
    }

    public SpringInterpreterFactory(ListableBeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.beanFactory = applicationContext;
    }

    @Override
    public Interpreter[] getInterpreters(StatementMetaData metaData) {
        if (interpreterFactory == null) {
            init();
        }
        return interpreterFactory.getInterpreters(metaData);
    }

    private void init() {
        synchronized (this) {
            if (interpreterFactory == null) {
                Map<String, Interpreter> map = beanFactory.getBeansOfType(Interpreter.class);
                ArrayList<Interpreter> interpreters = new ArrayList<Interpreter>(map.values());
                Collections.sort(interpreters, new InterpreterComparator());
                interpreterFactory = new DefaultInterpreterFactory(interpreters);
            }
        }
    }

}
