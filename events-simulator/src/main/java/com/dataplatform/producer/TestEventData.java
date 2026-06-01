package com.dataplatform.producer;

import java.util.List;

/**
 * Fixed pools used for random test events (accounts, users, operation types).
 */
public final class TestEventData {

    public static final List<String> ACCOUNT_IDS = List.of(
            "ACCOUNT-1", "ACCOUNT-2", "ACCOUNT-3", "ACCOUNT-4", "ACCOUNT-5");

    public static final List<String> USER_IDS = List.of(
            "USER-1", "USER-2", "USER-3", "USER-4", "USER-5");

    public static final List<String> OP_TYPES = List.of(
            "SamlLogin", "GetDashboardStats", "OAuthFlow", "UserLogin",
            "GetIntegration", "ListUsers", "GetUser", "CreateUser", "CreateGroup",
            "ListGroups", "ExpandGroup", "CreateIntegration", "ListIntegrations");

    private TestEventData() {
    }
}
