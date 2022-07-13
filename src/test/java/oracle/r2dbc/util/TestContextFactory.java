/*
  Copyright (c) 2020, 2022, Oracle and/or its affiliates.

  This software is dual-licensed to you under the Universal Permissive License
  (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License
  2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
  either license.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/
package oracle.r2dbc.util;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.spi.NamingManager;
import java.util.HashMap;
import java.util.Hashtable;

/**
 * <p>
 * A mock implementation of the JNDI
 * {@link javax.naming.spi.InitialContextFactory} SPI. This class is used for
 * testing LDAP URLs with Oracle R2DBC.
 *
 * </p><h3>Registering this Factory</h3><p>
 * When an LDAP URL is configured, the underlying Oracle JDBC Driver will do
 * something like this:
 * <pre>{@code
 *   var properties = new Properties()
 *   properties.setProperty(
 *     Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
 *   DirContext dirContext = new InitialDirContext(properties);
 * }</pre>
 * Note how JDBC has hardcoded the Sun LDAP factory. Test code needs to override
 * this, such that the DirContext object seen above will delegate to an instance
 * created by this test factory. This can be accomplished by registering a
 * factory builder that outputs an instance of this class:
 * <pre>{@code
 *   NamingManager.setInitialContextFactoryBuilder(environment ->
 *     new TestContextFactory());
 * }</pre>
 * When a factory builder is registered, the InitialDirContext constructor
 * ignores JDBC's configuration. Instead of using the Sun factory, the
 * constructor will use this test factory.
 *
 * </p><h3>Binding Oracle Net Descriptors</h3><p>
 * After Oracle JDBC gets the DirContext object (see previous section), it will
 * query it for an Oracle Net Descriptor. Oracle JDBC then uses this descriptor
 * to connect to Oracle Database (just as if the descriptor had been given as
 * a URL or as a tnsnames.ora entry). The name that JDBC queries for is
 * extracted from the path component of the LDAP URL. For example, if JDBC is
 * given an LDAP URL of:
 * <pre>
 *   ldap://ldap1.example.com:3500/cn=salesdept,cn=OracleContext,dc=com/salesdb
 * </pre>
 * Then Oracle JDBC queries the DirContext like this:
 * <pre>{@code
 * Attributes attributes = dirContext.getAttributes(
 *   "cn=salesdept,cn=OracleContext,dc=com/salesdb",
 *   new String[]{"orclnetdescstring"});
 * }</pre>
 * In the returned Attributes object, Oracle JDBC reads the descriptor
 * from the first value of the first attribute. An invocation of
 * {@link TestContextFactory#bind(String, String)} will bind a single attribute
 * with a single value to a given name. For example:
 * <pre>{@code
 * TestContextFactory.bind(
 *   "cn=salesdept,cn=OracleContext,dc=com/salesdb",
 *   "(DESCRIPTION=...)");
 * }</pre>
 * The code above would have JDBC connect to the given descriptor.
 * </p>
 */
public final class TestContextFactory
  implements javax.naming.spi.InitialContextFactory {

  static {
    // Set a factory builder that overrides any configuration of
    // Context.INITIAL_FACTORY_CONTEXT.
    try {
      NamingManager.setInitialContextFactoryBuilder(environment ->
        new TestContextFactory());
    }
    catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }
  /**
   * The {@code DirContext} object constructed by Oracle JDBC will delegate to
   * this instance of {@code TestDirContext}.
   */
  private static final TestDirContext DIR_CONTEXT = new TestDirContext();

  /**
   * {@inheritDoc}
   * Ignores the {@code environment} configured by Oracle JDBC, and just returns
   * the fixed instance of {@code TestDirContext}
   */
  @Override
  public Context getInitialContext(Hashtable<?, ?> environment)
    throws NamingException {
    TestDirContext.environment = (Hashtable<?, ?>) environment.clone();
    return DIR_CONTEXT;
  }

  /**
   * {@inheritDoc}
   * Binds the given name to the given value. This can be called to map an
   * Oracle Net Descriptor to the path of an LDAP URL.
   */
  public static void bind(String name, String value) {
    TestDirContext.ATTRIBUTES.put(name, new BasicAttributes(name, value));
  }

  /**
   * Implements the methods of the {@code DirContext} SPI which are called by
   * Oracle JDBC. All other method will throw an exception. This implementation
   * is simply backed by a mapping between names and attributes.
   */
  private static final class TestDirContext implements DirContext {

    /** Maps names to attributes */
    private static final HashMap<String, Attributes> ATTRIBUTES =
      new HashMap<>();

    /**
     * The environment of this context. It is passed to
     * {@link TestDirContext#getInitialContext(Hashtable)} when returning this
     * context.
     */
    private static Hashtable<?, ?> environment = new Hashtable<>();

    /**
     * {@inheritDoc}
     * Returns the attribute that has been mapped to a given name. Oracle JDBC
     * calls this method to get an Oracle Net Descriptor.
     */
    @Override
    public Attributes getAttributes(String name, String[] attrIds)
      throws NamingException {
      Attributes attributes = ATTRIBUTES.get(name);

      // It is noted that JDBC will prefix "cn=" on the path component of an
      // LDAP URL. Check if this is why the look up fails.
      if (attributes == null && name.startsWith("cn="))
        attributes = ATTRIBUTES.get(name.substring("cn=".length()));

      if (attributes == null)
        throw new NamingException("No attribute found for name: " + name);

      return attributes;
    }

    /**
     * {@inheritDoc}
     * JDBC calls this method when resolving a multi-endpoint LDAP URL.
     */
    @Override
    public Hashtable<?, ?> getEnvironment() throws NamingException {

      String providerUrl = (String)environment.get(Context.PROVIDER_URL);
      if (providerUrl != null) {

        // Replicating com.sun.jndi.ldap.LdapCtxFactory. When the provider URL is
        // a space-separated list, it returns the first working URL. JDBC then
        // calls getAttributes with the last path element that was mapped to this
        // URL. So if the working URL was: ldap://host:port/cn=.../db, then JDBC
        // calls getAttributes with "cn=db"
        String[] urls = providerUrl.split(" ");

        if (urls.length == 1)
          return environment;

        // Return the first URL. Don't include the ldap: scheme.
        var url = urls[0].substring(urls[0].indexOf('/'));
        @SuppressWarnings("unchecked")
        Hashtable<Object,Object> result =
          (Hashtable<Object,Object>)environment.clone();
        result.put(Context.PROVIDER_URL, url);
        return result;
      }

      return environment;
    }

    /**
     * {@inheritDoc}
     * Oracle JDBC calls this method after retrieving an Oracle Net Descriptor.
     * There are no resources to clean up.
     */
    @Override
    public void close() throws NamingException {
      // Do nothing
    }

    // Every method beyond this point is not called by Oracle JDBC. These
    // methods will all throw NamingException.

    @Override
    public Attributes getAttributes(Name name, String[] attrIds) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public Attributes getAttributes(Name name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public Attributes getAttributes(String name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void modifyAttributes(Name name, int mod_op, Attributes attrs) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void modifyAttributes(String name, int mod_op, Attributes attrs) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void modifyAttributes(Name name, ModificationItem[] mods) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void modifyAttributes(String name, ModificationItem[] mods) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void bind(Name name, Object obj, Attributes attrs) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void bind(String name, Object obj, Attributes attrs) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void rebind(Name name, Object obj, Attributes attrs) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void rebind(String name, Object obj, Attributes attrs) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public DirContext createSubcontext(Name name, Attributes attrs) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public DirContext createSubcontext(String name, Attributes attrs) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public DirContext getSchema(Name name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public DirContext getSchema(String name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public DirContext getSchemaClassDefinition(Name name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public DirContext getSchemaClassDefinition(String name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NamingEnumeration<SearchResult> search(Name name, Attributes matchingAttributes, String[] attributesToReturn) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NamingEnumeration<SearchResult> search(String name, Attributes matchingAttributes, String[] attributesToReturn) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NamingEnumeration<SearchResult> search(Name name, Attributes matchingAttributes) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NamingEnumeration<SearchResult> search(String name, Attributes matchingAttributes) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NamingEnumeration<SearchResult> search(Name name, String filter, SearchControls cons) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NamingEnumeration<SearchResult> search(String name, String filter, SearchControls cons) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NamingEnumeration<SearchResult> search(Name name, String filterExpr, Object[] filterArgs, SearchControls cons) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NamingEnumeration<SearchResult> search(String name, String filterExpr, Object[] filterArgs, SearchControls cons) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public Object lookup(Name name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public Object lookup(String name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void bind(Name name, Object obj) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void bind(String name, Object obj) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void rebind(Name name, Object obj) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void rebind(String name, Object obj) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void unbind(Name name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void unbind(String name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void rename(Name oldName, Name newName) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void rename(String oldName, String newName) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void destroySubcontext(Name name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public void destroySubcontext(String name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public Context createSubcontext(Name name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public Context createSubcontext(String name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public Object lookupLink(Name name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public Object lookupLink(String name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NameParser getNameParser(Name name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public NameParser getNameParser(String name) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public Name composeName(Name name, Name prefix) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public String composeName(String name, String prefix) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public Object addToEnvironment(String propName, Object propVal) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public Object removeFromEnvironment(String propName) throws NamingException {
      throw new NamingException("Not implemented");
    }

    @Override
    public String getNameInNamespace() throws NamingException {
      throw new NamingException("Not implemented");
    }
  }
}
