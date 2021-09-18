/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.bytecode;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 动态字节码的类加载器
 */
public class DynamicClassLoader
        extends ClassLoader
{
    private final ConcurrentMap<String, byte[]> pendingClasses = new ConcurrentHashMap<>();
    private final Map<Long, MethodHandle> callSiteBindings;
    private final Optional<ClassLoader> overrideClassLoader;

    public DynamicClassLoader(ClassLoader parentClassLoader)
    {
        this(parentClassLoader, ImmutableMap.of());
    }

    // TODO: this is a hack that should be removed
    @Deprecated
    public DynamicClassLoader(ClassLoader overrideClassLoader, ClassLoader parentClassLoader)
    {
        super(parentClassLoader);
        this.callSiteBindings = ImmutableMap.of();
        this.overrideClassLoader = Optional.of(overrideClassLoader);
    }

    public DynamicClassLoader(ClassLoader parentClassLoader, Map<Long, MethodHandle> callSiteBindings)
    {
        super(parentClassLoader);
        this.callSiteBindings = ImmutableMap.copyOf(callSiteBindings);
        this.overrideClassLoader = Optional.empty();
    }

    public Class<?> defineClass(String className, byte[] bytecode)
    {
        return defineClass(className, bytecode, 0, bytecode.length);
    }

    public Map<String, Class<?>> defineClasses(Map<String, byte[]> newClasses)
    {
        // 有冲突的类：已经存在的类，则表示冲突
        SetView<String> conflicts = Sets.intersection(pendingClasses.keySet(), newClasses.keySet());
        Preconditions.checkArgument(conflicts.isEmpty(), "The classes %s have already been defined", conflicts);

        // 将新的类字节码加入缓存
        pendingClasses.putAll(newClasses);
        try {
            // 将byte[]转化为Class<?>
            Map<String, Class<?>> classes = new HashMap<>();
            for (String className : newClasses.keySet()) {
                try {
                    // 加载类
                    Class<?> clazz = loadClass(className);
                    classes.put(className, clazz);
                }
                catch (ClassNotFoundException e) {
                    // this should never happen
                    throw new RuntimeException(e);
                }
            }
            return classes;
        }
        finally {
            pendingClasses.keySet().removeAll(newClasses.keySet());
        }
    }

    public Map<Long, MethodHandle> getCallSiteBindings()
    {
        return callSiteBindings;
    }

    /**
     * 重写其查找类方法，修改了其双亲委派机制
     * @param name
     * @return
     * @throws ClassNotFoundException
     */
    @Override
    protected Class<?> findClass(String name)
            throws ClassNotFoundException
    {
        // 从缓存中查找
        byte[] bytecode = pendingClasses.get(name);
        if (bytecode == null) {
            throw new ClassNotFoundException(name);
        }

        // 定义类
        return defineClass(name, bytecode);
    }

    /**
     * 加载类
     * @param name
     * @param resolve
     * @return
     * @throws ClassNotFoundException
     */
    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException
    {
        // grab the magic lock
        // 获取类加载锁
        synchronized (getClassLoadingLock(name)) {
            // Check if class is in the loaded classes cache
            // 是否存在缓存中
            Class<?> cachedClass = findLoadedClass(name);
            if (cachedClass != null) {
                // 解析类
                return resolveClass(cachedClass, resolve);
            }

            try {
                // 否则从重写的findClass()方法中查找类
                Class<?> clazz = findClass(name);
                return resolveClass(clazz, resolve);
            }
            catch (ClassNotFoundException ignored) {
                // not a local class
            }

            // 是否指定了其他类加载器，如果指定了使用指定的类加载器加载
            if (overrideClassLoader.isPresent()) {
                try {
                    return resolveClass(overrideClassLoader.get().loadClass(name), resolve);
                }
                catch (ClassNotFoundException e) {
                    // not in override loader
                }
            }

            // 加载类
            Class<?> clazz = getParent().loadClass(name);
            return resolveClass(clazz, resolve);
        }
    }

    private Class<?> resolveClass(Class<?> clazz, boolean resolve)
    {
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }
}
