package org.icij.datashare;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;
import org.neo4j.cypherdsl.core.Condition;
import org.neo4j.cypherdsl.core.Conditions;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.ExposesPatternLengthAccessors;
import org.neo4j.cypherdsl.core.ExposesRelationships;
import org.neo4j.cypherdsl.core.ExposesReturning;
import org.neo4j.cypherdsl.core.Expression;
import org.neo4j.cypherdsl.core.Node;
import org.neo4j.cypherdsl.core.PatternElement;
import org.neo4j.cypherdsl.core.Property;
import org.neo4j.cypherdsl.core.Relationship;
import org.neo4j.cypherdsl.core.RelationshipPattern;
import org.neo4j.cypherdsl.core.SortItem;
import org.neo4j.cypherdsl.core.Statement;
import org.neo4j.cypherdsl.core.StatementBuilder;

public class Neo4jUtils {

    protected static final String DOC_NODE = "Document";
    protected static final String DOC_PATH = "path";

    @JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.WRAPPER_OBJECT,
        visible = true
    )
    @JsonSubTypes({
        @JsonSubTypes.Type(value = PathPattern.class, name = "path"),
    })
    @JsonIgnoreProperties(value = {"@type"})
    protected interface Match extends Into<PatternElement> {
        public boolean isOptional();
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
    @JsonSubTypes({
        @JsonSubTypes.Type(value = SortByProperty.class, name = "byProperty"),
    })
    @JsonIgnoreProperties(value = {"@type"})
    protected interface OrderBy extends Into<SortItem> {
    }

    @JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.WRAPPER_OBJECT,
        visible = true
    )
    @JsonSubTypes({
        @JsonSubTypes.Type(value = And.class, name = "and"),
        @JsonSubTypes.Type(value = Or.class, name = "or"),
        @JsonSubTypes.Type(value = Not.class, name = "not"),
        @JsonSubTypes.Type(value = IsEqualTo.class, name = "isEqualTo"),
        @JsonSubTypes.Type(value = StartsWith.class, name = "startsWith"),
        @JsonSubTypes.Type(value = EndsWith.class, name = "endsWith")
    })
    @JsonIgnoreProperties(value = {"@type"})
    protected interface Where extends Into<Condition> {
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
    @JsonSubTypes({
        @JsonSubTypes.Type(value = VariableProperty.class),
        @JsonSubTypes.Type(value = LiteralWrapper.class),
    })
    protected interface WhereValue extends Into<Expression> {
    }

    protected interface Into<T> {
        T into();
    }

    protected static Statement documentSortToDumpStatement(
        List<Objects.DocumentSortItem> sort, long limit
    ) {
        Node doc = Cypher.node(DOC_NODE).named("doc");
        Node other = Cypher.anyNode().named("other");
        Relationship rel = doc.relationshipBetween(other).named("rel");
        SortItem[] orderBy = sort.stream().map(item -> {
            if (item.direction == Objects.SortDirection.ASC) {
                return doc.property(item.property).ascending();
            } else {
                return doc.property(item.property).descending();
            }
        }).toArray(SortItem[]::new);
        return Cypher.match(rel)
            .returning(doc, other, rel)
            .orderBy(orderBy)
            .limit(limit)
            .build();
    }

    protected static class Query {
        // TODO: make this generic, in order to support more match, where, orderBy and limit
        //  statement types... This could be done by implementing an Into<T> interface with a method
        //  into which will handle to conversion to PatternElement, Condition, SortItem
        public final List<Neo4jUtils.Match> matches;
        public final Neo4jUtils.Where where;
        public final List<Neo4jUtils.OrderBy> orderBy;
        // TODO: support Neo4jUtils.Into<Number>
        public final Long limit;

        @JsonCreator
        protected Query(
            @JsonProperty("matches") List<Neo4jUtils.Match> matches,
            @JsonProperty("where") Neo4jUtils.Where where,
            @JsonProperty("orderBy") List<Neo4jUtils.OrderBy> orderBy,
            @JsonProperty("limit") Long limit
        ) {
            java.util.Objects.requireNonNull(matches, "missing matches");
            if (matches.isEmpty()) {
                throw new IllegalArgumentException("empty matches");
            }
            this.matches = matches;
            this.where = where;
            this.orderBy = orderBy != null ? orderBy : List.of();
            this.limit = limit;
        }

        public Statement asValidated() {
            return this.validated(null);
        }

        public Statement asValidated(long defaultLimit) {
            return this.validated(defaultLimit);
        }

        protected Statement validated(Long defaultLimit) {
            StatementBuilder.OngoingReadingWithoutWhere statement = null;
            for (Match match : this.matches) {
                PatternElement pattern = match.into();
                if (statement == null) {
                    statement = match.isOptional() ? Cypher.optionalMatch(pattern) :
                        Cypher.match(pattern);
                } else {
                    statement = match.isOptional() ? statement.optionalMatch(pattern) :
                        statement.match(pattern);
                }
            }
            ExposesReturning returned;
            if (this.where != null) {
                returned = statement.where(this.where.into());
            } else {
                returned = statement;
            }
            Long limit;
            if (defaultLimit != null) {
                limit = defaultLimit;
                if (this.limit != null) {
                    limit = Math.min(this.limit, defaultLimit);
                }
            } else {
                limit = this.limit;
            }

            StatementBuilder.OngoingMatchAndReturnWithOrder returnedWithOrder =
                returned
                    .returning(Cypher.asterisk())
                    .orderBy(this.orderBy
                        .stream()
                        .map(Neo4jUtils.Into::into)
                        .collect(Collectors.toList()));
            if (limit == null) {
                return returnedWithOrder.build();
            }
            return returnedWithOrder
                .limit(limit)
                .build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Query dumpQuery = (Query) o;
            return java.util.Objects.equals(this.matches, dumpQuery.matches)
                && java.util.Objects.equals(this.where, dumpQuery.where)
                && java.util.Objects.equals(this.orderBy, dumpQuery.orderBy)
                && java.util.Objects.equals(this.limit, dumpQuery.limit);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(matches, where, orderBy, limit);
        }
    }

    protected static class SortByProperty implements OrderBy {
        protected final VariableProperty property;
        protected final Objects.SortDirection direction;

        @JsonCreator
        protected SortByProperty(
            @JsonProperty("property") VariableProperty property,
            @JsonProperty("direction") Objects.SortDirection direction
        ) {
            this.property = java.util.Objects.requireNonNull(property, "missing property");
            this.direction = java.util.Objects.requireNonNull(direction, "missing sort direction");
        }

        @Override
        public SortItem into() {
            org.neo4j.cypherdsl.core.Property sortByProp = this.property.into();
            if (this.direction.equals(Objects.SortDirection.ASC)) {
                return sortByProp.ascending();
            }
            return sortByProp.descending();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SortByProperty that = (SortByProperty) o;
            return java.util.Objects.equals(this.property, that.property)
                && this.direction == that.direction;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(property, direction);
        }
    }

    protected static class PatternNode implements Into<Node> {
        protected final String name;
        protected final List<String> labels;
        protected final Map<String, Object> properties;

        @JsonCreator
        protected PatternNode(
            @JsonProperty("name") String name,
            @JsonProperty("labels") List<String> labels,
            @JsonProperty("properties") Map<String, Object> properties
        ) {
            this.name = name;
            this.labels = labels;
            this.properties = properties;
        }

        @Override
        public Node into() {
            Node asNode;
            if (this.labels == null || this.labels.isEmpty()) {
                asNode = Cypher.anyNode();
            } else {
                asNode = Cypher.node(
                    this.labels.get(0), this.labels.subList(1, this.labels.size()));
            }
            if (this.name != null) {
                asNode = asNode.named(this.name);
            }
            if (this.properties != null && !this.properties.isEmpty()) {
                asNode = asNode.withProperties(this.properties);
            }
            return asNode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PatternNode that = (PatternNode) o;
            return java.util.Objects.equals(this.name, that.name)
                && java.util.Objects.equals(this.labels, that.labels)
                && java.util.Objects.equals(this.properties, that.properties);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, labels, properties);
        }
    }

    protected static class PatternRelationship {
        protected enum Direction {
            FROM,
            TO,
            BETWEEN;

            @JsonValue
            private String getId() {
                switch (this) {
                    case FROM:
                        return "from";
                    case TO:
                        return "to";
                    default:
                        return "between";
                }
            }
        }

        protected final String name;
        protected final Direction direction;
        protected final List<String> types;

        @JsonCreator
        protected PatternRelationship(
            @JsonProperty("name") String name,
            @JsonProperty("direction") Direction direction,
            @JsonProperty("types") List<String> types
        ) {

            this.name = name;
            this.direction = java.util.Objects.requireNonNull(
                direction, Direction.class.getName() + " expected");
            if (types == null) {
                types = new ArrayList<>(0);
            }
            this.types = types;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || this.getClass() != o.getClass()) {
                return false;
            }
            PatternRelationship that = (PatternRelationship) o;
            return java.util.Objects.equals(this.name, that.name)
                && this.direction == that.direction
                && java.util.Objects.equals(this.types, that.types);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, direction, types);
        }
    }

    protected static class PathPattern implements Match {
        // TODO: make this more generic... We should be able to accept many Node specifiers..
        protected final List<PatternNode> nodes;
        protected final List<PatternRelationship> relationships;
        protected final boolean optional;


        @JsonCreator
        protected PathPattern(
            @JsonProperty("nodes") List<PatternNode> nodes,
            @JsonProperty("relationships") List<PatternRelationship> relationships,
            @JsonProperty("optional") Boolean optional
        ) {
            int numNodes = nodes.size();
            if (numNodes > 0) {
                if (relationships == null) {
                    relationships = new ArrayList<>(0);
                }
                int numRelationships = relationships.size();
                if (numRelationships != numNodes - 1) {
                    String msg = "Invalid number of nodes and relationships, found "
                        + numNodes
                        + " nodes and "
                        + numRelationships
                        + " relationships";
                    throw new IllegalArgumentException(msg);
                }
            } else {
                throw new IllegalArgumentException("Path pattern must have at least one node");
            }
            this.nodes = nodes;
            this.relationships = relationships;
            this.optional = optional != null && optional;
        }

        @Override
        public PatternElement into() {
            Node first = this.nodes.get(0).into();
            if (nodes.size() == 1) {
                return first;
            }
            ListIterator<PatternRelationship> it = relationships.listIterator();
            PatternRelationship firstRel = it.next();
            RelationshipPattern chain = extendPatternChain(
                first, this.nodes.get(1).into(), firstRel);
            while (it.hasNext()) {
                PatternRelationship rel = it.next();
                Node rhsNode = this.nodes.get(it.nextIndex() + 1).into();
                chain = extendPatternChain(chain, rhsNode, rel);
            }
            return chain;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || this.getClass() != o.getClass()) {
                return false;
            }
            PathPattern that = (PathPattern) o;
            return java.util.Objects.equals(this.nodes, that.nodes)
                && java.util.Objects.equals(this.relationships, that.relationships);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(nodes, relationships);
        }

        @Override
        public boolean isOptional() {
            return this.optional;
        }
    }

    protected abstract static class PropertyWhere implements Where {
        protected final VariableProperty property;
        protected final WhereValue whereValue;

        @JsonCreator
        protected PropertyWhere(
            @JsonProperty("property") VariableProperty property,
            @JsonProperty("value") WhereValue value
        ) {
            this.property = java.util.Objects.requireNonNull(property, "missing property");
            this.whereValue = java.util.Objects.requireNonNull(value, "missing property value");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || this.getClass() != o.getClass()) {
                return false;
            }
            PropertyWhere that = (PropertyWhere) o;
            return java.util.Objects.equals(this.property, that.property)
                && java.util.Objects.equals(this.whereValue, that.whereValue);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(property, whereValue);
        }
    }

    protected static class VariableProperty implements WhereValue {
        protected final String variable;
        protected final String name;

        @JsonCreator
        protected VariableProperty(
            @JsonProperty("variable") String variable,
            @JsonProperty("name") String name
        ) {
            this.variable = java.util.Objects.requireNonNull(variable, "missing variable name");
            this.name = java.util.Objects.requireNonNull(name, "missing property name");
        }

        @Override
        public Property into() {
            return Cypher.property(this.variable, this.name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || this.getClass() != o.getClass()) {
                return false;
            }
            VariableProperty that = (VariableProperty) o;
            return java.util.Objects.equals(this.variable, that.variable)
                && java.util.Objects.equals(this.name, that.name);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(variable, name);
        }
    }

    protected static class LiteralWrapper implements WhereValue {

        protected final Object literal;

        @JsonCreator
        protected LiteralWrapper(@JsonProperty("literal") String literal) {
            this.literal = literal;
        }

        @Override
        public Expression into() {
            return Cypher.literalOf(this.literal);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || this.getClass() != o.getClass()) {
                return false;
            }
            LiteralWrapper that = (LiteralWrapper) o;
            return java.util.Objects.equals(this.literal, that.literal);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(literal);
        }
    }

    protected static class IsEqualTo extends PropertyWhere {
        @JsonCreator
        protected IsEqualTo(
            @JsonProperty("property") VariableProperty property,
            @JsonProperty("value") WhereValue value
        ) {
            super(property, value);
        }

        @Override
        public Condition into() {
            return this.property.into().isEqualTo(this.whereValue.into());
        }
    }


    protected static class StartsWith extends PropertyWhere {
        @JsonCreator
        protected StartsWith(
            @JsonProperty("property") VariableProperty property,
            @JsonProperty("value") WhereValue value
        ) {
            super(property, value);
        }

        @Override
        public Condition into() {
            return this.property.into().startsWith(this.whereValue.into());
        }
    }

    protected static class EndsWith extends PropertyWhere {
        @JsonCreator
        protected EndsWith(
            @JsonProperty("property") VariableProperty property,
            @JsonProperty("value") WhereValue value
        ) {
            super(property, value);
        }

        @Override
        public Condition into() {
            return this.property.into().endsWith(this.whereValue.into());
        }
    }

    protected static class And implements Where {


        @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        protected List<Where> conditions;

        @JsonCreator
        protected And(Where... conditions) {
            java.util.Objects.requireNonNull(conditions, "conditions expected");
            if (conditions.length == 0) {
                throw new IllegalArgumentException("empty where clause");
            }
            this.conditions = List.of(conditions);
        }


        @Override
        public Condition into() {
            ListIterator<Where> it = this.conditions.listIterator();
            Condition chain = null;
            while (it.hasNext()) {
                if (chain == null) {
                    chain = it.next().into();
                } else {
                    chain = chain.and(it.next().into());
                }
            }
            return chain;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || this.getClass() != o.getClass()) {
                return false;
            }
            And and = (And) o;
            return java.util.Objects.equals(this.conditions, and.conditions);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(conditions);
        }
    }

    protected static class Or implements Where {


        @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
        protected List<Where> conditions;

        @JsonCreator
        protected Or(Where... conditions) {
            java.util.Objects.requireNonNull(conditions, "conditions expected");
            if (conditions.length == 0) {
                throw new IllegalArgumentException("empty where clause");
            }
            this.conditions = List.of(conditions);
        }


        @Override
        public Condition into() {
            ListIterator<Where> it = this.conditions.listIterator();
            Condition chain = null;
            while (it.hasNext()) {
                if (chain == null) {
                    chain = it.next().into();
                } else {
                    chain = chain.or(it.next().into());
                }
            }
            return chain;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || this.getClass() != o.getClass()) {
                return false;
            }
            Or or = (Or) o;
            return java.util.Objects.equals(this.conditions, or.conditions);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(conditions);
        }
    }

    protected static class Not implements Where {

        protected Where value;

        @JsonCreator
        protected Not(@JsonProperty("value") Where value) {
            // TODO: skip the value if possible to allow {"not": {"isEqualTo": {...}}}
            //  instead of {"not": {"value": {"isEqualTo": {...}}}}
            java.util.Objects.requireNonNull(value, "expected value");
            this.value = value;
        }


        @Override
        public Condition into() {
            return Conditions.not(this.value.into());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || this.getClass() != o.getClass()) {
                return false;
            }
            Not that = (Not) o;
            return java.util.Objects.equals(this.value, that.value);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(this.value);
        }
    }


    static <T extends RelationshipPattern
        & ExposesPatternLengthAccessors<?>> T extendPatternChain(
        ExposesRelationships<T> chain,
        Node rhsNode,
        PatternRelationship relationship
    ) {
        String[] labels = relationship.types.toArray(new String[0]);
        T newChain;
        switch (relationship.direction) {
            case FROM:
                newChain = chain.relationshipFrom(rhsNode, labels);
                break;
            case TO:
                newChain = chain.relationshipTo(rhsNode, labels);
                break;
            default:
                newChain = chain.relationshipBetween(rhsNode, labels);
                break;
        }
        if (relationship.name != null) {
            newChain = (T) newChain.named(relationship.name);
        }
        return newChain;
    }


}
