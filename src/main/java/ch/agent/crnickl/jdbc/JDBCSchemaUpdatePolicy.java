package ch.agent.crnickl.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import ch.agent.crnickl.T2DBException;
import ch.agent.crnickl.T2DBMsg.D;
import ch.agent.crnickl.api.Property;
import ch.agent.crnickl.api.ValueType;
import ch.agent.crnickl.impl.SchemaUpdatePolicyImpl;
import ch.agent.crnickl.jdbc.T2DBJMsg.J;

public class JDBCSchemaUpdatePolicy extends SchemaUpdatePolicyImpl {

	private JDBCDatabaseMethods methods;
	
	public JDBCSchemaUpdatePolicy(JDBCDatabase database) {
		super(database);
		methods = database.getReadMethodsForProperty();
	}

	@Override
	public <T> void willDelete(Property<T> property) throws T2DBException {
		super.willDelete(property);
		if (countProperties(property) > 0)
			throw T2DBJMsg.exception(D.D20119, property.getName());
	}

	@Override
	public <T> void willDelete(ValueType<T> valueType) throws T2DBException {
		super.willDelete(valueType);
		if (countProperties(valueType) > 0)
			throw T2DBJMsg.exception(D.D10149, valueType.getName());
	}

	@Override
	public <T> void willDelete(ValueType<T> vt, T value)	throws T2DBException {
		super.willDelete(vt, value);
		String name = vt.getName();
		if (countDefaultValues(vt, vt.toString(value)) > 0)
			throw T2DBJMsg.exception(D.D10157, name, value);
		if (countActualValues(vt, vt.toString(value)) > 0)
			throw T2DBJMsg.exception(D.D10158, name, value);
	}

	private PreparedStatement count_default_values;
	private static final String COUNT_DEFAULT_VALUES = 
		"select count(*) from " + DB.SCHEMA_ITEM + " s, " + DB.PROPERTY + " p " + 
		"where s.prop = p.id and p.type = ? and s.value = ?";
	private <T> int countDefaultValues(ValueType<T> vt, String value) throws T2DBException {
		try {
			count_default_values = methods.open(COUNT_DEFAULT_VALUES, vt, count_default_values);
			count_default_values.setInt(1, methods.getId(vt));
			count_default_values.setString(2, value);
			ResultSet rs = count_default_values.executeQuery();
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw T2DBJMsg.exception(e, J.J10107, vt.getName());
		} finally {
			count_default_values = methods.close(count_default_values);
		}
	}
	
	private PreparedStatement count_actual_values;
	private static final String COUNT_ACTUAL_VALUES = 
		"select count(*) from " + DB.PROPERTY + " p, " + DB.ATTRIBUTE_VALUE + " a " + 
		" where a.value = ? and p.type = ? and p.id = a.prop";
	private <T>int countActualValues(ValueType<T> vt, String value) throws T2DBException {
		try {
			count_actual_values = methods.open(COUNT_ACTUAL_VALUES, vt, count_actual_values);
			count_actual_values.setString(1, value);
			count_actual_values.setInt(2, methods.getId(vt));
			ResultSet rs = count_actual_values.executeQuery();
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw T2DBJMsg.exception(e, J.J10107, vt.getName());
		} finally {
			count_actual_values = methods.close(count_actual_values);
		}
	}

	private PreparedStatement count_property;
	private static final String COUNT_PROPERTY = 
		"select count(*) from " + DB.PROPERTY + " where type = ?";
	private int countProperties(ValueType<?> vt) throws T2DBException {
		try {
			count_property = methods.open(COUNT_PROPERTY, vt, count_property);
			count_property.setInt(1, methods.getId(vt));
			ResultSet rs = count_property.executeQuery();
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw T2DBJMsg.exception(e, J.J10107, vt.getName());
		} finally {
			count_property = methods.close(count_property);
		}
	}

	private PreparedStatement count_slot;
	private static final String COUNT_SLOT = 
			"select count(*) from " + DB.SCHEMA_ITEM + " where prop = ?";
	private <T>int countProperties(Property<T> property) throws T2DBException {
		try {
			count_slot = methods.open(COUNT_SLOT, property, count_slot);
			count_slot.setInt(1, methods.getId(property));
			ResultSet rs = count_slot.executeQuery();
			rs.next();
			return rs.getInt(1);
		} catch (SQLException e) {
			throw T2DBJMsg.exception(e, J.J20107, property.getName());
		} finally {
			count_slot = methods.close(count_slot);
		}
	}

	
}
