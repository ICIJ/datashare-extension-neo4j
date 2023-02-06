package org.icij.datashare;

import net.codestory.http.security.Users;
import org.icij.datashare.user.User;

public class DatashareUser extends User implements net.codestory.http.security.User {
    public DatashareUser(User user) {
        super(user);
    }

    @Override
    public String login() {
        return null;
    }

    @Override
    public String[] roles() {
        return new String[0];
    }

    static Users singleUser(String userId) {
        return new Users() {
            @Override
            public net.codestory.http.security.User find(String s, String s1) {
                return s.equals(userId) ? new DatashareUser(User.localUser(userId)) : null;
            }

            @Override
            public net.codestory.http.security.User find(String s) {
                return s.equals(userId) ? new DatashareUser(User.localUser(userId)) : null;
            }
        };
    }

}
