package com.linkedin.thirdeye.taskexecution.dag;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang.StringUtils;


/**
 * An identifier to stores the name of a node or an operator. Note that this class is mutable because the namespace of
 * an identifier could be updated: Be caution when using this class as hash keys.
 */
public class NodeIdentifier {
  public static final String NAMESPACE_SEPARATOR = ":";

  private String name = "";
  private List<String> namespaces = new ArrayList<>();
  // For fast comparison and calculation of hashcode
  private String fullName = "";

  public NodeIdentifier() {
  }

  public NodeIdentifier(String name) {
    setName(name);
  }

  public NodeIdentifier(String namespace, String name) {
    setNamespace(namespace);
    setName(name);
  }

  public String getName() {
    return name;
  }

  private void setName(String name) {
    Preconditions.checkNotNull(name);
    if (!this.name.equals(name)) {
      this.name = name;
      updateFullName();
    }
  }

  public String getNamespace() {
    return Joiner.on(NAMESPACE_SEPARATOR).skipNulls().join(namespaces);
  }

  public void setNamespace(String namespace) {
    Preconditions.checkNotNull(namespace, "namespace cannot be a null string.");
    this.namespaces = new ArrayList<>(Arrays.asList(namespace.split(NAMESPACE_SEPARATOR)));
    updateFullName();
  }

  public void addParentNamespace(String namespace) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(namespace), "namespace cannot be a null or empty string.");
    List<String> parentsNamespace = new ArrayList<>(Arrays.asList(namespace.split(NAMESPACE_SEPARATOR)));
    if (parentsNamespace.size() > 0) {
      parentsNamespace.addAll(namespaces);
      namespaces = parentsNamespace;
      updateFullName();
    }
  }

  private void updateFullName() {
    Preconditions.checkNotNull(name, "Unable to update full name because name string is null.");
    String namespace = getNamespace();
    if (StringUtils.isNotBlank(namespace)) {
      fullName = namespace + NAMESPACE_SEPARATOR + name;
    } else {
      fullName = name;
    }
  }

  public String getFullName() {
    return fullName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeIdentifier that = (NodeIdentifier) o;
    return Objects.equals(getFullName(), that.getFullName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getFullName());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("NodeIdentifier{");
    sb.append(getFullName()).append('}');
    return sb.toString();
  }
}
