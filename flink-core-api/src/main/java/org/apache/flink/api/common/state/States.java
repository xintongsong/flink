/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.typeinfo.TypeDescriptor;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** All related classes of state declaration. */
public class States {

    public static <T> ListStateDeclaration<T> listState(
            String name, TypeDescriptor<T> typeDescriptor) {
        return new ListStateDeclaration<>(name, typeDescriptor, null);
    }

    public static <T> ListStateDeclaration<T> splitRedistributionListState(
            String name, TypeDescriptor<T> typeDescriptor) {
        return new ListStateDeclaration<>(
                name, typeDescriptor, ListStateDeclaration.RedistributionStrategy.SPLIT);
    }

    public static <T> ListStateDeclaration<T> unionRedistributionListState(
            String name, TypeDescriptor<T> typeDescriptor) {
        return new ListStateDeclaration<>(
                name, typeDescriptor, ListStateDeclaration.RedistributionStrategy.UNION);
    }

    public static <T> ValueStateDeclaration<T> valueState(
            String name, TypeDescriptor<T> typeDescriptor) {
        return new ValueStateDeclaration<>(name, typeDescriptor);
    }

    public static <K, V> MapStateDeclaration<K, V> broadcastMapState(
            String name,
            TypeDescriptor<K> keyTypeDescriptor,
            TypeDescriptor<V> valueTypeDescriptor) {
        return new MapStateDeclaration<>(
                name, keyTypeDescriptor, valueTypeDescriptor, RedistributionMode.IDENTICAL);
    }

    // TODO: implement above, and move everything below to implementation modules

    /** Declaration for state. */
    public abstract static class StateDeclaration implements Serializable {
        private final String name;

        private final RedistributionMode redistributionMode;

        public StateDeclaration(String name, RedistributionMode redistributionMode) {
            this.name = name;
            this.redistributionMode = redistributionMode;
        }

        public String getName() {
            return name;
        }

        public RedistributionMode getRedistributionMode() {
            return redistributionMode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StateDeclaration that = (StateDeclaration) o;
            return Objects.equals(getName(), that.getName());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getName());
        }
    }

    public static class ListStateDeclaration<T> extends StateDeclaration {

        private final TypeDescriptor<T> elementTypeDescriptor;

        @Nullable private final RedistributionStrategy redistributionStrategy;

        private ListStateDeclaration(
                String name,
                TypeDescriptor<T> elementTypeDescriptor,
                @Nullable RedistributionStrategy redistributionStrategy) {
            super(
                    name,
                    redistributionStrategy == null
                            ? RedistributionMode.NONE
                            : RedistributionMode.REDISTRIBUTABLE);
            this.elementTypeDescriptor = elementTypeDescriptor;
            this.redistributionStrategy = redistributionStrategy;
        }

        public TypeDescriptor<T> getElementTypeDescriptor() {
            return elementTypeDescriptor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ListStateDeclaration<?> that = (ListStateDeclaration<?>) o;
            return Objects.equals(getElementTypeDescriptor(), that.getElementTypeDescriptor());
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), getElementTypeDescriptor());
        }

        enum RedistributionStrategy {
            SPLIT,
            UNION
        }
    }

    public static class ValueStateDeclaration<T> extends StateDeclaration {

        private final TypeDescriptor<T> typeDescriptor;

        private ValueStateDeclaration(String name, TypeDescriptor<T> typeDescriptor) {
            super(name, RedistributionMode.NONE);
            this.typeDescriptor = typeDescriptor;
        }

        public TypeDescriptor<T> getTypeDescriptor() {
            return typeDescriptor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            ValueStateDeclaration<?> that = (ValueStateDeclaration<?>) o;
            return Objects.equals(typeDescriptor, that.typeDescriptor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), typeDescriptor);
        }
    }

    public static class MapStateDeclaration<K, V> extends StateDeclaration {
        private final TypeDescriptor<K> keyTypeDescriptor;
        private final TypeDescriptor<V> valueTypeDescriptor;

        public MapStateDeclaration(
                String name,
                TypeDescriptor<K> keyTypeDescriptor,
                TypeDescriptor<V> valueTypeDescriptor,
                RedistributionMode redistributionMode) {
            super(name, redistributionMode);
            this.keyTypeDescriptor = keyTypeDescriptor;
            this.valueTypeDescriptor = valueTypeDescriptor;
        }
    }

    enum RedistributionMode {
        NONE,
        REDISTRIBUTABLE,
        IDENTICAL
    }
}
