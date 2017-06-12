/**
 * This file is part of Graylog Beats Plugin.
 *
 * Graylog Beats Plugin is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog Beats Plugin is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog Beats Plugin.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog.plugins.beats;

import org.graylog.plugins.beats.input.BeatKafkaInput;
import org.graylog.plugins.beats.input.KafkaTransport;
import org.graylog2.plugin.PluginModule;

public class BeatsInputPluginModule extends PluginModule {
    @Override
    protected void configure() {
        addTransport("Kafka beat input", KafkaTransport.class);
        addTransport("beats", BeatsTransport.class);
        addCodec("beats", BeatsCodec.class);
        addMessageInput(BeatsInput.class);
        addMessageInput(BeatKafkaInput.class);
    }
}
