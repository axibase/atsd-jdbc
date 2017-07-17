package com.axibase.tsd.driver.jdbc.util;

import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class WildcardsUtil {
	private static final char ONE_ANY_SYMBOL = '_';
	private static final char NONE_OR_MORE_SYMBOLS = '%';
	private static final char ATSD_WILDCARD = '*';
	private static final int NOT_FOUND = -1;

	public static boolean hasWildcards(String text) {
		return text == null || text.indexOf(ONE_ANY_SYMBOL) != NOT_FOUND || text.indexOf(NONE_OR_MORE_SYMBOLS) != NOT_FOUND;
	}

	public static boolean wildcardMatch(String text, String pattern) {
		return wildcardMatch(text, pattern, ONE_ANY_SYMBOL, NONE_OR_MORE_SYMBOLS);
	}

	public static boolean atsdWildcardMatch(String text, String pattern) {
		return wildcardMatch(text, pattern, '\0', ATSD_WILDCARD);
	}

	private static boolean wildcardMatch(String text, String pattern, char anySymbol, char manySymbol) {
		if (pattern == null) {
			return true;
		}
		if (text == null) {
			return false;
		}

		final String oneAnySymbolStr = String.valueOf(anySymbol);
		final String noneOrMoreSymbolsStr = String.valueOf(manySymbol);
		final int stringLength = text.length();
		final String[] wildcardTokens = splitOnTokens(pattern, oneAnySymbolStr, noneOrMoreSymbolsStr);
		boolean anyChars = false;
		int textIdx = 0;
		int wildcardTokensIdx = 0;
		final List<BacktrackContext> backtrack = new ArrayList<>();

		// loop around a backtrack stack to handle complex % matching
		do {
			final int backtrackListSize = backtrack.size();
			if (backtrackListSize > 0) {
				BacktrackContext context = backtrack.remove(backtrackListSize - 1);
				wildcardTokensIdx = context.tokenIndex;
				textIdx = context.charIndex;
				anyChars = true;
			}

			// loop whilst tokens and text left to process
			while (wildcardTokensIdx < wildcardTokens.length) {
				final String wildcardToken = wildcardTokens[wildcardTokensIdx];
				if (oneAnySymbolStr.equals(wildcardToken)) {
					// found one-symbol mask, hence move to next text char
					++textIdx;
					if (textIdx > stringLength) {
						break;
					}
					anyChars = false;
				} else if (noneOrMoreSymbolsStr.equals(wildcardToken)) {
					anyChars = true;
					if (wildcardTokensIdx == wildcardTokens.length - 1) {
						textIdx = stringLength;
					}
				} else {
					if (anyChars) {
						// any chars, hence try to locate text token
						textIdx = StringUtils.indexOfIgnoreCase(text, wildcardToken, textIdx);
						if (textIdx == NOT_FOUND) {
							break;
						}
						int repeatIdx = StringUtils.indexOfIgnoreCase(text, wildcardToken, textIdx  +1);
						if (repeatIdx >= 0) {
							backtrack.add(new BacktrackContext(wildcardTokensIdx, repeatIdx));
						}
					} else {
						// matching from current position
						if (!text.regionMatches(true, textIdx, wildcardToken, 0, wildcardToken.length())) {
							// couldn't match token
							break;
						}
					}

					// matched text token, move text index to end of matched token
					textIdx += wildcardToken.length();
					anyChars = false;
				}

				++wildcardTokensIdx;
			}

			// full match
			if (wildcardTokensIdx == wildcardTokens.length && textIdx == text.length()) {
				return true;
			}

		} while (!backtrack.isEmpty());

		return false;
	}

	/**
	 * Splits a string into a number of tokens.
	 * The text is split by '_' and '%'.
	 * Multiple '%' are  are collapsed into a single '%'.
	 *
	 * @param text  the text to split
	 * @return the array of tokens, never null
	 */
	private static String[] splitOnTokens(String text, String oneAnySymbolStr, String noneOrMoreSymbolsStr) {
		if (!hasWildcards(text)) {
			return new String[] { text };
		}
		List<String> list = new ArrayList<>();
		StringBuilder buffer = new StringBuilder();

		final int length = text.length();
		for (int i = 0; i < length; i++) {
			final char current = text.charAt(i);

			switch (current) {
				case ONE_ANY_SYMBOL:
					flushBuffer(buffer, list);
					if (!list.isEmpty() && noneOrMoreSymbolsStr.equals(list.get(list.size() - 1))) {
						list.set(list.size() - 1, oneAnySymbolStr);
						list.add(noneOrMoreSymbolsStr);
					} else {
						list.add(oneAnySymbolStr);
					}
					break;
				case NONE_OR_MORE_SYMBOLS:
					flushBuffer(buffer, list);
					if (list.isEmpty() || !noneOrMoreSymbolsStr.equals(list.get(list.size() - 1))) {
						list.add(noneOrMoreSymbolsStr);
					}
					break;
				default:
					buffer.append(current);
			}
		}
		if (buffer.length() != 0) {
			list.add(buffer.toString());
		}

		return list.toArray(new String[list.size()]);
	}

	public static String replaceSqlWildcardsWithAtsd(String text) {
		if (text == null) {
			return null;
		}
		final int textLength = text.length();
		StringBuilder buffer = new StringBuilder(textLength);
		for (int i = 0; i < textLength; ++i) {
			final char currentChar = text.charAt(i);
			if (currentChar == ONE_ANY_SYMBOL || currentChar == NONE_OR_MORE_SYMBOLS) {
				if (i == 0 || buffer.charAt(buffer.length() - 1) != ATSD_WILDCARD) {
					buffer.append(ATSD_WILDCARD);
				}
			} else {
				buffer.append(currentChar);
			}
		}
		return buffer.toString();
	}

	private static void flushBuffer(StringBuilder buffer, List<String> list) {
		if (buffer.length() != 0) {
			list.add(buffer.toString());
			buffer.setLength(0);
		}
	}

	@AllArgsConstructor
	private static final class BacktrackContext {
		private final int tokenIndex;
		private final int charIndex;
	}
}
