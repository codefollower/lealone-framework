/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.orm;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.constraint.ConstraintReferential;
import org.lealone.db.index.Index;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.DataType;
import org.lealone.db.value.ReadonlyArray;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.plugins.orm.json.JsonObject;
import org.lealone.plugins.orm.property.PBase;
import org.lealone.plugins.orm.property.PLong;
import org.lealone.sql.dml.Delete;
import org.lealone.sql.dml.Insert;
import org.lealone.sql.dml.Update;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.SelectOrderBy;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.Wildcard;
import org.lealone.sql.expression.aggregate.Aggregate;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.sql.query.Select;
import org.lealone.transaction.Transaction;

/**
 * 所有关系表会生成一个 Model 子类，这个类提供 crud 和 join 操作
 *
 * @param <T> Model 子类
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class Model<T extends Model<T>> {

    public static final short REGULAR_MODEL = 0;
    public static final short ROOT_DAO = 1;
    public static final short CHILD_DAO = 2;

    public enum CaseFormat {
        CAMEL,
        LOWER_UNDERSCORE,
        UPPER_UNDERSCORE
    }

    private static final Logger logger = LoggerFactory.getLogger(Model.class);

    private static final ConcurrentSkipListMap<Long, ServerSession> currentSessions = new ConcurrentSkipListMap<>();
    private static final ConcurrentSkipListMap<Integer, List<ServerSession>> sessionMap = new ConcurrentSkipListMap<>();

    private static class Stack<E> {

        private final LinkedList<E> list = new LinkedList<>();

        public void push(E item) {
            list.offerFirst(item);
        }

        public E pop() {
            return list.pollFirst();
        }

        public E peek() {
            return list.peekFirst();
        }

        public boolean isEmpty() {
            return list.isEmpty();
        }

        public int size() {
            return list.size();
        }

        public E first() {
            return list.peekLast();
        }
    }

    private static class NVPair {

        public final String name;
        public final Value value;

        public NVPair(String name, Value value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            NVPair other = (NVPair) obj;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            return true;
        }
    }

    private static class PRowId<M extends Model<M>> extends PLong<M> {
        public PRowId(M root) {
            super(Column.ROWID, root);
        }

        @Override
        public Long get() {
            return value == null ? 0 : value;
        }
    }

    private final PRowId _rowid_ = new PRowId(this);

    // The root model instance. Used to provide fluid query construction.
    private final T root;

    private final ModelTable modelTable;
    private short modelType; // 0: regular model; 1: root dao; 2: child dao
    private ModelProperty[] modelProperties;
    private ArrayList<Model<?>> modelList;
    private HashMap<Class, ArrayList<Model<?>>> modelMap;
    private HashMap<String, ModelProperty> nameToModelPropertyMap;

    // 以下字段不是必须的，所以延迟初始化，避免浪费不必要的内存
    private HashMap<String, NVPair> nvPairs;
    private ArrayList<Expression> selectExpressions;
    private ArrayList<Expression> groupExpressions;
    private ExpressionBuilder<T> having;
    private ExpressionBuilder<T> whereExpressionBuilder;

    private Expression limitExpr;
    private Expression offsetExpr;

    // The underlying expression builders held as a stack. Pushed and popped based on and/or.
    private Stack<ExpressionBuilder<T>> expressionBuilderStack;
    private Stack<TableFilter> tableFilterStack;

    protected Model(ModelTable table, short modelType) {
        root = (T) this;
        this.modelTable = table;
        this.modelType = modelType;
    }

    ModelTable getModelTable() {
        return modelTable;
    }

    public String getDatabaseName() {
        return modelTable.getDatabaseName();
    }

    public String getSchemaName() {
        return modelTable.getSchemaName();
    }

    public String getTableName() {
        return modelTable.getTableName();
    }

    public boolean isDao() {
        return modelType > 0;
    }

    private boolean isRootDao() {
        return modelType == ROOT_DAO;
    }

    protected void setModelProperties(ModelProperty[] modelProperties) {
        this.modelProperties = modelProperties;
        nameToModelPropertyMap = new HashMap<>(modelProperties.length);
        for (ModelProperty p : modelProperties) {
            nameToModelPropertyMap.put(p.getName(), p);
        }
    }

    ModelProperty getModelProperty(String name) {
        return nameToModelPropertyMap.get(name);
    }

    protected T addModel(Model<?> m) {
        if (modelList == null) {
            modelList = new ArrayList<>();
        }
        if (modelMap == null) {
            modelMap = new HashMap<>();
        }
        ArrayList<Model<?>> list = modelMap.get(m.getClass());
        if (list == null) {
            list = new ArrayList<>();
            modelMap.put(m.getClass(), list);
        }
        modelList.add(m);
        list.add(m);
        return root;
    }

    protected <M> List<M> getModelList(Class c) {
        ArrayList<Model<?>> oldList = modelMap.get(c);
        if (oldList == null) {
            return null;
        }
        ArrayList<Model<?>> newList = new ArrayList<>(oldList.size());
        HashMap<Long, Long> map = new HashMap<>(oldList.size());
        for (Model<?> m : oldList) {
            Long id = m._rowid_.get();
            if (map.put(id, id) == null) {
                newList.add(m);
            }
        }
        return (List<M>) newList;
    }

    void addNVPair(String name, Value value) {
        if (nvPairs == null) {
            nvPairs = new HashMap<>();
        }
        nvPairs.put(name, new NVPair(name, value));
    }

    private void reset() {
        nvPairs = null;
        selectExpressions = null;
        groupExpressions = null;
        having = null;
        whereExpressionBuilder = null;
        expressionBuilderStack = null;
        tableFilterStack = null;
    }

    private ExpressionBuilder<T> getWhereExpressionBuilder() {
        if (whereExpressionBuilder == null) {
            whereExpressionBuilder = new ExpressionBuilder<T>(root);
        }
        return whereExpressionBuilder;
    }

    private ArrayList<Expression> getSelectExpressions() {
        if (selectExpressions == null) {
            selectExpressions = new ArrayList<>();
        }
        return selectExpressions;
    }

    public final T select(ModelProperty<?>... properties) {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.select(properties);
        }
        selectExpressions = new ArrayList<>();
        for (ModelProperty<?> p : properties) {
            ExpressionColumn c = getExpressionColumn(p);
            selectExpressions.add(c);
        }
        return root;
    }

    public T orderBy() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.orderBy();
        }
        joinTableFilter();
        getStack().pop();
        pushExprBuilder(getWhereExpressionBuilder());
        return root;
    }

    public final T groupBy(ModelProperty<?>... properties) {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.groupBy(properties);
        }
        groupExpressions = new ArrayList<>();
        for (ModelProperty<?> p : properties) {
            ExpressionColumn c = getExpressionColumn(p);
            groupExpressions.add(c);
        }
        return root;
    }

    private static ExpressionColumn getExpressionColumn(Table table, String cName) {
        return new ExpressionColumn(table.getDatabase(), table.getSchema().getName(), table.getName(),
                cName);
    }

    static ExpressionColumn getExpressionColumn(ModelProperty<?> p) {
        return new ExpressionColumn(p.getDatabaseName(), p.getSchemaName(), p.getTableName(),
                p.getName());
    }

    static ExpressionColumn getExpressionColumn(TableFilter tableFilter, String propertyName) {
        return new ExpressionColumn(tableFilter.getTable().getDatabase(), tableFilter.getSchemaName(),
                tableFilter.getTableAlias(), propertyName);
    }

    ExpressionColumn getExpressionColumn(String propertyName) {
        return new ExpressionColumn(modelTable.getDatabase(), modelTable.getSchemaName(),
                modelTable.getTableName(), propertyName);
    }

    public T having() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.having();
        }
        getStack().pop();
        having = new ExpressionBuilder<>(root);
        pushExprBuilder(having);
        return root;
    }

    public T or() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.or();
        }
        peekExprBuilder().or();
        return root;
    }

    public T and() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.and();
        }
        peekExprBuilder().and();
        return root;
    }

    public T not() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.not();
        }
        peekExprBuilder().not();
        return root;
    }

    private void checkDao(String methodName) {
        if (!isDao()) {
            throw new UnsupportedOperationException(
                    "The " + methodName + " operation is not allowed, please use "
                            + this.getClass().getSimpleName() + ".dao." + methodName + "() instead.");
        }
    }

    public T where() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.where();
        }
        joinTableFilter();
        return root;
    }

    private void joinTableFilter() {
        if (tableFilterStack != null) {
            TableFilter first = tableFilterStack.first();
            while (tableFilterStack.size() > 1) {
                ExpressionBuilder<T> on = getStack().pop();
                TableFilter joined = getTableFilterStack().pop();
                first.addJoin(joined, false, on.getExpression());
            }
        }
    }

    /**
     * Execute the query returning either a single model or null (if no matching model is found).
     */
    public T findOne() {
        return findOne(null);
    }

    public T findOne(Long tid) {
        checkDao("findOne");
        // 进行关联查询时，主表取一条记录，但引用表要取多条
        if (tableFilterStack != null && !tableFilterStack.isEmpty()) {
            List<T> list = findList();
            if (list.isEmpty()) {
                return null;
            } else {
                return list.get(0);
            }
        }
        Select select = createSelect(tid);
        select.setLimit(ValueExpression.get(ValueInt.get(1)));
        select.init();
        select.prepare();
        logger.info("execute sql: " + select.getPlanSQL());
        Result result = select.executeQuery(1).get();
        result.next();
        reset();

        Map<Class<?>, Map<Long, Model<?>>> map = new LinkedHashMap<>();
        String[] fieldNames = getFieldNames(result);
        Set<Model<?>> set = getAllAssociateInstances(fieldNames);
        deserialize(result, fieldNames, set, map);
        Map<Long, Model<?>> models = map.get(this.getClass());
        if (models != null) {
            for (Model<?> m : models.values()) {
                m.bindAssociateInstances(map);
                return ((T) m);
            }
        }
        return null;
    }

    // 如果select字段列表中没有加上引用约束的字段，那么自动加上
    private HashMap<String, ExpressionColumn> getRefConstraintColumns(HashSet<Table> tables) {
        CaseInsensitiveMap<ExpressionColumn> columnMap = new CaseInsensitiveMap<>();
        CaseInsensitiveMap<ExpressionColumn> selectMap = new CaseInsensitiveMap<>(
                selectExpressions.size());
        for (Expression e : selectExpressions) {
            if (e instanceof ExpressionColumn) {
                ExpressionColumn c = (ExpressionColumn) e;
                selectMap.put(c.getAlias(), c);
            }
        }

        for (Table table : tables) {
            for (ConstraintReferential ref : table.getReferentialConstraints()) {
                Table refTable = ref.getRefTable();
                Table owner = ref.getTable();
                if (tables.contains(refTable) && tables.contains(owner)) {
                    IndexColumn[] refColumns = ref.getRefColumns();
                    IndexColumn[] columns = ref.getColumns();
                    for (int i = 0; i < columns.length; i++) {
                        String name = columns[i].column.getName();
                        String refName = refColumns[i].column.getName();
                        String key = owner.getName() + "." + name;
                        String refKey = refTable.getName() + "." + refName;
                        if (!columnMap.containsKey(key) && !selectMap.containsKey(key)) {
                            columnMap.put(key, getExpressionColumn(owner, name));
                        }
                        if (!columnMap.containsKey(refKey) && !selectMap.containsKey(refKey)) {
                            columnMap.put(refKey, getExpressionColumn(refTable, refName));
                        }
                    }
                }
            }
        }
        return columnMap;
    }

    private Select createSelect(Long tid) {
        ServerSession session = getSession(tid);
        Select select = new Select(session);
        TableFilter tableFilter;
        if (tableFilterStack != null && !tableFilterStack.isEmpty()) {
            if (tableFilterStack.size() > 1) {
                // 表join时，如果没加where条件，在这里把TableFilter连在一起
                joinTableFilter();
            }
            HashSet<Table> tables;
            tableFilter = tableFilterStack.peek();
            select.addTableFilter(tableFilter, true);
            boolean selectExpressionsIsNull = false;
            if (selectExpressions == null) {
                tables = null;
                selectExpressionsIsNull = true;
                getSelectExpressions()
                        .add(new Wildcard(tableFilter.getSchemaName(), tableFilter.getTableAlias()));
            } else {
                tables = new HashSet<>();
                tables.add(tableFilter.getTable());
            }
            selectExpressions.add(getExpressionColumn(tableFilter, Column.ROWID)); // 总是获取rowid
            while (tableFilter.getJoin() != null) {
                select.addTableFilter(tableFilter.getJoin(), false);
                tableFilter = tableFilter.getJoin();
                if (selectExpressionsIsNull)
                    selectExpressions
                            .add(new Wildcard(tableFilter.getSchemaName(), tableFilter.getTableAlias()));
                else
                    tables.add(tableFilter.getTable());
                selectExpressions.add(getExpressionColumn(tableFilter, Column.ROWID)); // 总是获取rowid
            }
            if (!selectExpressionsIsNull)
                selectExpressions.addAll(getRefConstraintColumns(tables).values());
        } else {
            tableFilter = new TableFilter(session, modelTable.getTable(), null, true, null);
            select.addTableFilter(tableFilter, true);
            if (selectExpressions == null) {
                getSelectExpressions().add(new Wildcard(null, null));
            }
            selectExpressions.add(getExpressionColumn(Column.ROWID)); // 总是获取rowid
        }
        select.setExpressions(selectExpressions);
        if (whereExpressionBuilder != null)
            select.addCondition(whereExpressionBuilder.getExpression());
        if (groupExpressions != null) {
            select.setGroupQuery();
            select.setGroupBy(groupExpressions);
            select.setHaving(having.getExpression());
        }
        if (whereExpressionBuilder != null)
            select.setOrder(whereExpressionBuilder.getOrderList());

        if (limitExpr != null)
            select.setLimit(limitExpr);
        if (offsetExpr != null)
            select.setOffset(offsetExpr);

        return select;
    }

    private void deserialize(Result result, String[] fieldNames, Set<Model<?>> set,
            Map<Class<?>, Map<Long, Model<?>>> topMap) {
        Value[] row = result.currentRow();
        if (row == null)
            return;

        int len = row.length;
        HashMap<String, Value> map = new HashMap<>(len);
        for (int i = 0; i < len; i++) {
            // 只反序列化非null字段
            if (row[i] != null && row[i] != ValueNull.INSTANCE) {
                map.put(fieldNames[i], row[i]);
            }
        }
        for (Model<?> m : set) {
            m = m.newInstance(m.modelTable, REGULAR_MODEL);
            m._rowid_.deserialize(map);
            if (m._rowid_.get() == 0) {
                DbException.throwInternalError();
            }
            Model<?> old = putIfAbsent(topMap, m);
            if (old == null) {
                for (ModelProperty p : m.modelProperties) {
                    p.deserialize(map);
                }
            }
        }
    }

    private Model putIfAbsent(Map<Class<?>, Map<Long, Model<?>>> topMap, Model m) {
        Map<Long, Model<?>> models = topMap.get(m.getClass());
        Model old = null;
        if (models == null) {
            models = new LinkedHashMap<>();
            topMap.put(m.getClass(), models);
        } else {
            old = models.get(m._rowid_.get());
        }
        if (old == null) {
            models.put(m._rowid_.get(), m);
        }
        return old;
    }

    protected Model newInstance(ModelTable t, short modelType) {
        return null;
    }

    protected static boolean areEqual(PBase<?, ?> p1, PBase<?, ?> p2) {
        return ModelProperty.areEqual(p1.get(), p2.get());
    }

    private CaseFormat getCaseFormat() {
        String cf = modelTable.getTable().getParameter("caseFormat");
        CaseFormat format;
        if (cf == null)
            format = CaseFormat.UPPER_UNDERSCORE;
        else
            format = CaseFormat.valueOf(cf.toUpperCase());
        return format;
    }

    public Map<String, Object> toMap() {
        return toMap(null);
    }

    public Map<String, Object> toMap(CaseFormat format) {
        if (format == null)
            format = getCaseFormat();
        Map<String, Object> map = new LinkedHashMap<>();
        for (ModelProperty<?> p : modelProperties) {
            p.serialize(map, format);
        }
        if (modelMap != null) {
            for (Entry<Class, ArrayList<Model<?>>> e : modelMap.entrySet()) {
                map.put(e.getKey().getSimpleName() + "List", e.getValue());
            }
        }
        map.put("modelType", modelType);
        return map;
    }

    public String encode() {
        return encode(null);
    }

    public String encode(CaseFormat format) {
        return new JsonObject(toMap(format)).encode();
    }

    protected T decode0(String str) {
        return decode0(str, null);
    }

    protected T decode0(String str, CaseFormat format) {
        if (format == null)
            format = getCaseFormat();
        Map<String, Object> map = new JsonObject(str).getMap();
        for (ModelProperty<?> p : modelProperties) {
            Object v = map.get(p.getName(format));
            if (v != null) {
                // 先反序列化再set，这样Model的子类对象就可以在后续调用insert之类的方法
                p.deserializeAndSet(v);
            }
        }
        Object v = map.get("modelType");
        if (v == null) {
            modelType = REGULAR_MODEL;
        } else {
            modelType = ((Number) v).shortValue();
        }
        return (T) this;
    }

    /**
     * Execute the query returning the list of objects.
     */
    public List<T> findList() {
        return findList(null);
    }

    public List<T> findList(Long tid) {
        checkDao("findList");
        Select select = createSelect(tid);
        select.init();
        select.prepare();
        logger.info("execute sql: " + select.getPlanSQL());
        Result result = select.executeQuery(-1).get();
        reset();

        Map<Class<?>, Map<Long, Model<?>>> map = new HashMap<>();
        String[] fieldNames = getFieldNames(result);
        Set<Model<?>> set = getAllAssociateInstances(fieldNames);
        while (result.next()) {
            deserialize(result, fieldNames, set, map);
        }
        ArrayList<T> list = new ArrayList<>(result.getRowCount());
        Map<Long, Model<?>> models = map.get(this.getClass());
        if (models != null) {
            for (Model<?> m : models.values()) {
                m.bindAssociateInstances(map);
                list.add((T) m);
            }
        }
        return list;
    }

    private String[] getFieldNames(Result result) {
        int len = result.getVisibleColumnCount();
        String[] fieldNames = new String[len];
        for (int i = 0; i < len; i++) {
            fieldNames[i] = result.getSchemaName(i) + "." + result.getTableName(i) + "."
                    + result.getColumnName(i);
        }
        return fieldNames;
    }

    public Array findArray() {
        return findArray(null);
    }

    public Array findArray(Long tid) {
        List<T> list = findList(tid);
        int size = list.size();
        Object[] values = new Object[size];
        for (int i = 0; i < size; i++) {
            values[i] = list.get(i).encode();
        }
        return new ReadonlyArray(DataType.convertToValue(values, Value.ARRAY));
    }

    public <M extends Model<M>> M m(Model<M> m) {
        Model<T> m2 = maybeCopy();
        if (m2 != this) {
            return m2.m(m);
        }
        Model<T> old = peekExprBuilder().getOldModel();
        if (!old.isRootDao() && m.getClass() == old.getClass()) {
            m = (Model<M>) old;
        } else {
            Model<M> m3 = m.maybeCopy();
            if (m3 != m) {
                m = m3;
            }
        }
        m.tableFilterStack = this.tableFilterStack;
        m.whereExpressionBuilder = (ExpressionBuilder<M>) this.whereExpressionBuilder;
        peekExprBuilder().setModel((T) m);
        m.pushExprBuilder((ExpressionBuilder<M>) peekExprBuilder());
        return m.root;
    }

    /**
     * Return the count of entities this query should return.
     */
    public int findCount() {
        return findCount(null);
    }

    public int findCount(Long tid) {
        checkDao("findCount");
        Select select = createSelect(tid);
        select.setGroupQuery();
        getSelectExpressions().clear();
        Aggregate a = Aggregate.create(Aggregate.COUNT_ALL, null, select, false);
        getSelectExpressions().add(a);
        select.setExpressions(getSelectExpressions());
        select.init();
        select.prepare();
        logger.info("execute sql: " + select.getPlanSQL());
        Result result = select.executeQuery(-1).get();
        reset();
        result.next();
        return result.currentRow()[0].getInt();
    }

    private ServerSession getSession(Long tid) {
        boolean autoCommit = false;
        ServerSession session;
        if (tid != null) {
            session = currentSessions.get(tid);
        } else {
            session = peekSession();
        }
        if (session == null) {
            session = modelTable.getSession();
            autoCommit = true;
        } else {
            autoCommit = false;
        }

        session.setAutoCommit(autoCommit);
        return session;
    }

    public long insert() {
        return insert(null);
    }

    public long insert(Long tid) {
        // 必须设置字段值
        if (nvPairs == null) {
            throw new UnsupportedOperationException("No values insert");
        }
        // 不允许通过 X.dao来insert记录
        if (isDao()) {
            String name = this.getClass().getSimpleName();
            throw new UnsupportedOperationException("The insert operation is not allowed for " + name
                    + ".dao,  please use new " + name + "().insert() instead.");
        }
        // 批量提交子model需要在一个事务中执行
        if (modelList != null) {
            tid = beginTransaction();
        }
        ServerSession session = getSession(tid);
        Table dbTable = modelTable.getTable();
        Insert insert = new Insert(session);
        int size = nvPairs.size();
        Column[] columns = new Column[size];
        Expression[] expressions = new Expression[size];
        int i = 0;
        for (NVPair p : nvPairs.values()) {
            columns[i] = dbTable.getColumn(p.name);
            expressions[i] = ValueExpression.get(p.value);
            i++;
        }
        insert.setColumns(columns);
        insert.addRow(expressions);
        insert.setTable(dbTable);
        insert.prepare();
        logger.info("execute sql: " + insert.getPlanSQL());
        insert.executeUpdate();
        long rowId = session.getLastIdentity().getLong(); // session.getLastRowKey()在事务提交时被设为null了
        _rowid_.set(rowId);

        if (modelList != null) {
            try {
                for (Model<?> m : modelList) {
                    m.insert(tid);
                }
                commitTransaction(tid);
            } catch (Exception e) {
                rollbackTransaction(tid);
                throw DbException.convert(e);
            }
        }
        if (session.isAutoCommit()) {
            session.commit();
        }
        reset();
        return rowId;
    }

    public int update() {
        return update(null);
    }

    public int update(Long tid) {
        // 没有变化，直接返回0
        if (nvPairs == null) {
            return 0;
        }
        ServerSession session = getSession(tid);
        Table dbTable = modelTable.getTable();
        Update update = new Update(session);
        TableFilter tableFilter = new TableFilter(session, dbTable, null, true, null);
        update.setTableFilter(tableFilter);
        checkWhereExpression(dbTable, "update");
        if (whereExpressionBuilder != null)
            update.setCondition(whereExpressionBuilder.getExpression());
        for (NVPair p : nvPairs.values()) {
            update.setAssignment(dbTable.getColumn(p.name), ValueExpression.get(p.value));
        }
        update.prepare();
        reset();
        logger.info("execute sql: " + update.getPlanSQL());
        int count = update.executeUpdate().get();
        if (session.isAutoCommit()) {
            session.commit();
        }
        return count;
    }

    public int delete() {
        return delete(null);
    }

    public int delete(Long tid) {
        ServerSession session = getSession(tid);
        Table dbTable = modelTable.getTable();
        Delete delete = new Delete(session);
        TableFilter tableFilter = new TableFilter(session, dbTable, null, true, null);
        delete.setTableFilter(tableFilter);
        checkWhereExpression(dbTable, "delete");
        if (whereExpressionBuilder != null)
            delete.setCondition(whereExpressionBuilder.getExpression());
        delete.prepare();
        reset();
        logger.info("execute sql: " + delete.getPlanSQL());
        int count = delete.executeUpdate().get();
        if (session.isAutoCommit()) {
            session.commit();
        }
        return count;
    }

    private void checkWhereExpression(Table dbTable, String methodName) {
        if (whereExpressionBuilder == null || whereExpressionBuilder.getExpression() == null) {
            maybeCreateWhereExpression(dbTable);
            if (whereExpressionBuilder == null || whereExpressionBuilder.getExpression() == null) {
                checkDao(methodName);
            }
        } else {
            checkDao(methodName);
        }
    }

    Model<T> maybeCopy() {
        if (isRootDao()) {
            return newInstance(modelTable.copy(), CHILD_DAO);
        } else {
            return this;
        }
    }

    private void maybeCreateWhereExpression(Table dbTable) {
        // 没有指定where条件时，如果存在ROWID，则用ROWID当where条件
        if (_rowid_.get() != 0) {
            peekExprBuilder().eq(Column.ROWID, _rowid_.get());
        } else {
            if (nvPairs == null)
                return;
            Index primaryKey = dbTable.findPrimaryKey();
            if (primaryKey != null) {
                for (Column c : primaryKey.getColumns()) {
                    // 如果主键由多个字段组成，当前面的字段没有指定时就算后面的指定了也不用它们来生成where条件
                    boolean found = false;
                    for (NVPair p : nvPairs.values()) {
                        if (dbTable.getDatabase().equalsIdentifiers(p.name, c.getName())) {
                            peekExprBuilder().eq(p.name, p.value);
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                        break;
                }
            }
        }
    }

    private Stack<ExpressionBuilder<T>> getStack() {
        if (expressionBuilderStack == null) {
            expressionBuilderStack = new Stack<ExpressionBuilder<T>>();
            expressionBuilderStack.push(getWhereExpressionBuilder());
        }
        return expressionBuilderStack;
    }

    private TableFilter createTableFilter() {
        return new TableFilter(modelTable.getSession(), modelTable.getTable(), null, true, null);
    }

    private Stack<TableFilter> getTableFilterStack() {
        if (tableFilterStack == null) {
            TableFilter tableFilter = createTableFilter();
            tableFilterStack = new Stack<>();
            tableFilterStack.push(tableFilter);
        }
        return tableFilterStack;
    }

    /**
     * Push the expression builder onto the appropriate stack.
     */
    private T pushExprBuilder(ExpressionBuilder<T> builder) {
        getStack().push(builder);
        return root;
    }

    /**
     * Return the current expression builder that expressions should be added to.
     */
    ExpressionBuilder<T> peekExprBuilder() {
        return getStack().peek();
    }

    public T limit(long v) {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.limit(v);
        }
        limitExpr = ValueExpression.get(ValueLong.get(v));
        return root;
    }

    public T offset(long v) {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.offset(v);
        }
        offsetExpr = ValueExpression.get(ValueLong.get(v));
        return root;
    }

    /**
     * left parenthesis
     */
    public T lp() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.lp();
        }
        ExpressionBuilder<T> e = new ExpressionBuilder<>(root);
        pushExprBuilder(e);
        return root;
    }

    /**
     * right parenthesis
     */
    public T rp() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.rp();
        }
        ExpressionBuilder<T> right = getStack().pop();
        ExpressionBuilder<T> left = peekExprBuilder();
        left.junction(right);
        return root;
    }

    public T join(Model<?> m) {
        Model<T> m2 = maybeCopy();
        if (m2 != this) {
            return m2.join(m);
        }
        getTableFilterStack().push(m.createTableFilter());
        m.tableFilterStack = getTableFilterStack();
        return root;
    }

    public T on() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.on();
        }
        ExpressionBuilder<T> e = new ExpressionBuilder<>(root);
        pushExprBuilder(e);
        return root;
    }

    public long beginTransaction() {
        // checkDao("beginTransaction");
        Table dbTable = modelTable.getTable();
        ServerSession session = dbTable.getDatabase().createSession(modelTable.getSession().getUser());
        Transaction t = session.getTransaction();
        session.setAutoCommit(false);
        long tid = t.getTransactionId();
        currentSessions.put(tid, session);
        int hash = getCurrentThreadHashCode();
        List<ServerSession> sessions = sessionMap.get(hash);
        if (sessions == null) {
            sessions = new ArrayList<>();
            sessionMap.put(hash, sessions);
        }
        sessions.add(session);
        return tid;
    }

    public void commitTransaction() {
        // checkDao("commitTransaction");
        Long tid = getAndRemoveLastTransaction();
        if (tid != null) {
            commitTransaction(tid.longValue());
        }
    }

    public void commitTransaction(long tid) {
        // checkDao("commitTransaction");
        ServerSession s = currentSessions.remove(tid);
        if (s != null) {
            removeSession(tid);
            s.commit();
        }
    }

    public void rollbackTransaction() {
        // checkDao("rollbackTransaction");
        Long tid = getAndRemoveLastTransaction();
        if (tid != null) {
            rollbackTransaction(tid.longValue());
        }
    }

    public void rollbackTransaction(long tid) {
        // checkDao("rollbackTransaction");
        ServerSession s = currentSessions.remove(tid);
        if (s != null) {
            removeSession(tid);
            s.rollback();
        }
    }

    private ServerSession peekSession() {
        int hash = getCurrentThreadHashCode();
        List<ServerSession> sessions = sessionMap.get(hash);
        if (sessions != null && !sessions.isEmpty()) {
            return sessions.get(sessions.size() - 1);
        } else {
            return null;
        }
    }

    private void removeSession(long tid) {
        int hash = getCurrentThreadHashCode();
        List<ServerSession> sessions = sessionMap.get(hash);
        if (sessions != null && !sessions.isEmpty()) {
            int index = -1;
            for (ServerSession s : sessions) {
                index++;
                if (s.getTransaction().getTransactionId() == tid) {
                    break;
                }
            }
            if (index > -1) {
                sessions.remove(index);
            }
            if (sessions.isEmpty()) {
                sessionMap.remove(hash);
            }
        }
    }

    private Long getAndRemoveLastTransaction() {
        int hash = getCurrentThreadHashCode();
        List<ServerSession> sessions = sessionMap.remove(hash);
        Long tid = null;
        if (sessions != null && !sessions.isEmpty()) {
            ServerSession session = sessions.remove(sessions.size() - 1);
            tid = Long.valueOf(session.getTransaction().getTransactionId());
        }
        return tid;
    }

    private int getCurrentThreadHashCode() {
        return Thread.currentThread().hashCode();
    }

    @Override
    public String toString() {
        return new JsonObject(toMap()).encodePrettily();
    }

    public void printSQL() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        if (selectExpressions != null) {
            sql.append(selectExpressions.get(0).getSQL());
            for (int i = 1, size = selectExpressions.size(); i < size; i++)
                sql.append(", ").append(selectExpressions.get(i).getSQL());
        } else {
            sql.append("*");
        }
        sql.append(" FROM ").append(modelTable.getTableName());
        if (whereExpressionBuilder != null) {
            sql.append("\r\n  WHERE ").append(whereExpressionBuilder.getExpression().getSQL());
        }
        if (groupExpressions != null) {
            sql.append("\r\n  GROUP BY (").append(groupExpressions.get(0).getSQL());
            for (int i = 1, size = groupExpressions.size(); i < size; i++)
                sql.append(", ").append(groupExpressions.get(i).getSQL());
            sql.append(")");
            if (having != null) {
                sql.append(" HAVING ").append(having.getExpression().getSQL());
            }
        }
        if (whereExpressionBuilder != null) {
            ArrayList<SelectOrderBy> list = whereExpressionBuilder.getOrderList();
            if (list != null && !list.isEmpty()) {
                sql.append("\r\n  ORDER BY (").append(list.get(0).getSQL());
                for (int i = 1, size = list.size(); i < size; i++)
                    sql.append(", ").append(list.get(i).getSQL());
                sql.append(")");
            }
        }
        System.out.println(sql);
    }

    ////////////////////// 以下代码从结果集构建出模型实例后，再把模型实例彼此的关联关系绑定 /////////////////////

    private Set<Model<?>> getAllAssociateInstances(String[] fieldNames) {
        HashMap<Class<?>, Model<?>> map = new HashMap();
        getAllAssociateInstances(map);
        HashSet<Model<?>> set = new HashSet<>();
        HashSet<String> names = new HashSet<>(Arrays.asList(fieldNames));
        for (Model<?> m : map.values()) {
            // 结果集中没有model对应的表的记录，不用处理
            if (names.contains(m._rowid_.getFullName())) {
                set.add(m);
            }
        }
        return set;
    }

    private void getAllAssociateInstances(HashMap<Class<?>, Model<?>> map) {
        if (map.containsKey(getClass()))
            return;
        map.put(getClass(), this);

        if (setters != null) {
            getAllAssociateInstances(map, setters);
        }
        if (adders != null) {
            getAllAssociateInstances(map, adders);
        }
    }

    private void getAllAssociateInstances(HashMap<Class<?>, Model<?>> map, AssociateOperation... aos) {
        for (AssociateOperation ao : aos) {
            ao.getDao().getAllAssociateInstances(map);
        }
    }

    private AssociateSetter[] setters;
    private AssociateAdder[] adders;

    protected void initSetters(AssociateSetter... setters) {
        this.setters = setters;
    }

    protected void initAdders(AssociateAdder... adders) {
        this.adders = adders;
    }

    protected static interface AssociateOperation<T extends Model> {
        public T getDao();
    }

    protected static interface AssociateSetter<T extends Model> extends AssociateOperation<T> {
        public boolean set(T m);
    }

    protected static interface AssociateAdder<T extends Model> extends AssociateOperation<T> {
        public void add(T m);
    }

    private boolean bound;

    private void bindAssociateInstances(Map<Class<?>, Map<Long, Model<?>>> map) {
        if (bound)
            return;
        bound = true;
        if (setters != null) {
            for (AssociateSetter setter : setters) {
                Map<Long, Model<?>> models = map.get(setter.getDao().getClass());
                if (models != null) {
                    for (Model<?> m : models.values()) {
                        m.bindAssociateInstances(map);
                        if (setter.set(m)) {
                            break;
                        }
                    }
                }
            }
        }
        if (adders != null) {
            for (AssociateAdder adder : adders) {
                Map<Long, Model<?>> models = map.get(adder.getDao().getClass());
                if (models != null) {
                    for (Model<?> m : models.values()) {
                        m.bindAssociateInstances(map);
                        adder.add(m);
                    }
                }
            }
        }
    }
}
